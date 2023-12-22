package txblaster

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bitcoin-sv/ubsv/cmd/txblaster/worker"
	_ "github.com/bitcoin-sv/ubsv/k8sresolver"
	"github.com/bitcoin-sv/ubsv/services/coinbase"
	"github.com/bitcoin-sv/ubsv/services/p2p"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/distributor"
	"github.com/libsv/go-p2p/wire"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc/resolver"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
)

const progname = "tx-blaster"

// // Version & commit strings injected at build with -ldflags -X...
var version string
var commit string

var logger ulogger.Logger

var printProgress uint64

var kafkaProducer sarama.SyncProducer
var kafkaTopic string
var ipv6MulticastConn *net.UDPConn
var ipv6MulticastChan = make(chan worker.Ipv6MulticastMsg)
var totalTransactions atomic.Uint64
var startTime time.Time

const privateKeyFilename = "tx-blaster.private_key"

// var subscription *pubsub.Subscription
// var p2pHost host.Host
var topic *pubsub.Topic

func Start() {
	gocore.SetInfo(progname, version, commit)

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger = ulogger.New("txblast", ulogger.WithLevel(logLevelStr))

	_ = os.Chdir("../../")

	ctx, cancelFunc := context.WithCancel(context.Background())

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		<-sigs

		cancelFunc() // cancel the contexts and wait for all to stop
		time.Sleep(1 * time.Second)
		logger.Infof("TX Blaster finished, total transactions: ~%d", totalTransactions.Load())
		os.Exit(0)
	}()

	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	workers := flag.Int("workers", runtime.NumCPU(), "how many workers to use for blasting")
	rateLimit := flag.Float64("limit", -1, "rate limit tx/s per worker")
	printFlag := flag.Int("print", 0, "print out progress every x transactions")
	kafka := flag.String("kafka", "", "Kafka server URL - if applicable")
	ipv6Address := flag.String("ipv6Address", "", "IPv6 multicast address - if applicable")
	ipv6Interface := flag.String("ipv6Interface", "en0", "IPv6 multicast interface - if applicable")
	profileAddress := flag.String("profile", "", "use this profile port instead of the default")
	logIds := flag.Bool("log", false, "log tx ids")
	useQuic := flag.Bool("quic", false, "use quic and invalid tx subscription")

	flag.Parse()

	prometheusEndpoint, ok := gocore.Config().Get("prometheusEndpoint")
	if ok && prometheusEndpoint != "" {
		logger.Infof("Starting prometheus endpoint on %s", prometheusEndpoint)
		http.Handle(prometheusEndpoint, promhttp.Handler())
	}

	if gocore.Config().GetBool("use_open_tracing", true) {
		logger.Infof("Starting open tracing")
		serviceName, _ := gocore.Config().Get("SERVICE_NAME", "tx-blaster")
		samplingRateStr, _ := gocore.Config().Get("tracing_SampleRate", "0.01")
		samplingRate, err := strconv.ParseFloat(samplingRateStr, 64)
		if err != nil {
			logger.Errorf("error parsing sampling rate: %v", err)
			samplingRate = 0.01
		}

		_, closer, err := util.InitGlobalTracer(serviceName, samplingRate)
		if err != nil {
			panic(err)
		}

		defer closer.Close()
	}
	var txDistributor *distributor.Distributor
	var err error
	if !*useQuic {
		logger.Debugf("Using grpc distributor")
		txDistributor, err = distributor.NewDistributor(logger,
			distributor.WithBackoffDuration(200*time.Millisecond),
			distributor.WithRetryAttempts(3),
			distributor.WithFailureTolerance(0),
		)
		if err != nil {
			logger.Fatalf("error creating tx distributor: %v", err)
		}
	}

	coinbaseClient, err := coinbase.NewClient(ctx, logger)
	if err != nil {
		logger.Fatalf("error creating coinbase tracker client: %v", err)
	}

	if kafka != nil && *kafka != "" {
		logger.Infof("Connecting to kafka at %s", *kafka)
		kafkaURL, err := url.Parse(*kafka)
		if err != nil {
			logger.Fatalf("unable to parse kafka url: %v", err)
		}

		clusterAdmin, producer, err := util.ConnectToKafka(kafkaURL)
		if err != nil {
			logger.Fatalf("unable to connect to kafka: %v", err)
		}

		defer func() {
			_ = clusterAdmin.Close()
			_ = producer.Close()
		}()

		kafkaProducer = producer
		kafkaTopic = kafkaURL.Path[1:]
	}

	if ipv6Address != nil && *ipv6Address != "" {
		logger.Infof("Using %s ipv6Address", *ipv6Address)
		logger.Infof("Using ipv6 multicast interface %s at address %s", *ipv6Interface, *ipv6Address)
		en0, err := net.InterfaceByName(*ipv6Interface)
		if err != nil {
			logger.Fatalf("error resolving interface: %v", err)
		}

		addr := &net.UDPAddr{
			IP:   net.ParseIP(*ipv6Address),
			Port: 9999,
			Zone: en0.Name,
		}

		logger.Infof("Starting IPv6 multicast on %s", addr.String())
		ipv6MulticastConn, err = net.DialUDP("udp6", nil, addr)
		if err != nil {
			logger.Fatalf("error dialing address: %v", err)
		}

		go func() {
			for {
				msg := <-ipv6MulticastChan

				r := bytes.NewReader(msg.TxExtendedBytes)
				msgTx := &wire.MsgExtendedTx{}
				err = msgTx.Deserialize(r)
				if err != nil {
					logger.Errorf("error deserializing tx %s: %v", utils.ReverseAndHexEncodeSlice(msg.IDBytes), err)
					continue
				}

				if err = wire.WriteMessage(msg.Conn, msgTx, wire.ProtocolVersion, wire.MainNet); err != nil {
					if errors.Is(err, io.EOF) {
						logger.Infof("[%s] Connection closed", msg.Conn.RemoteAddr())
						continue
					}
					logger.Errorf("[%s] Failed to write message: %v", msg.Conn.RemoteAddr(), err)
				}
			}
		}()
	}

	if *useQuic {
		topicPrefix, ok := gocore.Config().Get("p2p_topic_prefix")
		if !ok {
			panic("p2p_topic_prefix not set in config")
		}

		rtn, ok := gocore.Config().Get("p2p_rejected_tx_topic")
		if !ok {
			panic("p2p_rejected_tx_topic not set in config")
		}
		rejectedTxTopicName := fmt.Sprintf("%s-%s", topicPrefix, rtn)

		topic, err = createlibp2pTopic(ctx, rejectedTxTopicName)
		if err != nil {
			panic(err)
		}
	}

	printProgress = uint64(*printFlag)

	go func() {
		var profilerAddr string
		var startProfiler bool

		if profileAddress != nil && *profileAddress != "" {
			profilerAddr, startProfiler = *profileAddress, true
		} else {
			profilerAddr, startProfiler = gocore.Config().Get("tx_blaster_profilerAddr", ":9191")
		}

		if startProfiler {
			logger.Infof("Starting profile on http://%s/debug/pprof", profilerAddr)
			logger.Fatalf("%v", http.ListenAndServe(profilerAddr, nil))
		}
	}()

	grpcResolver, _ := gocore.Config().Get("grpc_resolver")
	if grpcResolver == "k8s" {
		logger.Infof("[VALIDATOR] Using k8s resolver for clients")
		resolver.SetDefaultScheme("k8s")
	} else if grpcResolver == "kubernetes" {
		logger.Infof("[VALIDATOR] Using kubernetes resolver for clients")
		kuberesolver.RegisterInClusterWithSchema("k8s")
	}
	if !*useQuic {
		propagationServers := txDistributor.GetPropagationGRPCAddresses()
		if len(propagationServers) == 0 {
			panic("No suitable propagation server connection found")
		}

		logger.Infof("Using %d propagation servers: %+v", len(propagationServers), propagationServers)
		logger.Infof("Starting %d workers", *workers)
	}
	var logIdsFile chan string
	if *logIds {
		logFile, err := os.OpenFile("data/txblaster.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}

		logIdsFile = make(chan string, 100000)
		go func() {
			for id := range logIdsFile {
				_, _ = logFile.WriteString(id + "\n")
			}
		}()
	}

	startTime = time.Now()

	// start http health check server
	http.Handle("/health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))

	for i := 0; i < *workers; i++ {
		if *useQuic {
			// create a quic distributor for each worker
			txDistributor, err = distributor.NewQuicDistributor(logger,
				distributor.WithBackoffDuration(200*time.Millisecond),
				distributor.WithRetryAttempts(3),
				distributor.WithFailureTolerance(0),
			)
			if err != nil {
				logger.Errorf("error creating tx quic distributor for worker %d: %v", i, err)
				continue
			}
		}
		workerLogger := logger.New(fmt.Sprintf("wrk_%d", i))
		go startWorker(ctx, workerLogger, i, *rateLimit, coinbaseClient, txDistributor, logIdsFile)
		// stagger worker startup to not overload Coinbase
		time.Sleep(100 * time.Millisecond)
	}

	<-ctx.Done()
}

func startWorker(ctx context.Context, logger ulogger.Logger, workerId int, rateLimit float64,
	coinbaseClient *coinbase.Client, txDistributor *distributor.Distributor, logIdsFile chan string) {

	var w *worker.Worker
	var err error

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if rateLimit > 0 {
				logger.Infof("starting worker %d with rate limit: %0.2f/s", workerId, rateLimit)
			} else {
				logger.Infof("starting worker %d", workerId)
			}

			w, err = worker.NewWorker(
				logger,
				rateLimit,
				coinbaseClient,
				txDistributor,
				kafkaProducer,
				kafkaTopic,
				ipv6MulticastConn,
				ipv6MulticastChan,
				printProgress,
				logIdsFile,
				&totalTransactions,
				&startTime,
				topic,
			)
			if err != nil {
				logger.Errorf("Could not initialise worker %d: %v", workerId, err)
				continue
			}

			err = w.Init(ctx)
			if err != nil {
				logger.Errorf("Could not initialise worker %d: %v", workerId, err)
				continue
			}

			// start will only return if an error occurs
			if err = w.Start(ctx); err != nil {
				logger.Errorf("error from worker: %v", err)
			}

			time.Sleep(1 * time.Second)
		}
	}
}

func discoverPeers(ctx context.Context, topicName string, h host.Host) {
	kademliaDHT := p2p.InitDHT(ctx, h)
	routingDiscovery := drouting.NewRoutingDiscovery(kademliaDHT)

	dutil.Advertise(ctx, routingDiscovery, topicName)

	// Look for others who have announced and attempt to connect to them
	anyConnected := false
ConnectLoop:
	for {
		select {
		case <-ctx.Done():
			logger.Infof("P2P service shutting down")
			return
		default:
			if !anyConnected {
				logger.Debugf("Searching for peers for topic %s..\n", topicName)
				time.Sleep(1 * time.Second)
				peerChan, err := routingDiscovery.FindPeers(ctx, topicName)
				if err != nil {
					panic(err)
				}

				for p := range peerChan {
					if p.ID == h.ID() {
						continue // No self connection
					}
					err = h.Connect(ctx, p)
					if err != nil {
						//  we fail to connect to a lot of peers. Just ignore it for now.
						// s.logger.Debugf("Failed connecting to ", peer.ID.Pretty(), ", error:", err)
					} else {
						logger.Debugf("Connected to:", p.ID.String())
						anyConnected = true
					}
				}

			} else {
				logger.Debugf("Peer discovery complete")
				logger.Debugf("connected to %d peers\n", len(h.Network().Peers()))
				logger.Debugf("peerstore has %d peers\n", len(h.Peerstore().Peers()))
				break ConnectLoop
			}
		}
	}
}

func createlibp2pTopic(ctx context.Context, topicName string) (*pubsub.Topic, error) {
	logger.Debugf("Starting libp2pListener. topicName=%s, workerId:%d", topicName)

	var pk *crypto.PrivKey
	var err error

	pk, err = readPrivateKey()
	if err != nil {
		pk, err = generatePrivateKey()
		if err != nil {
			return nil, err
		}
	}
	// Create a new libp2p Host that listens on a random TCP port
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"), libp2p.Identity(*pk))
	if err != nil {
		return nil, err
	}

	go discoverPeers(ctx, topicName, h)
	// Set up a new PubSub service using the GossipSub router
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		return nil, err
	}

	topic, err := ps.Join(topicName)
	if err != nil {
		return nil, err
	}

	return topic, err
}

func generatePrivateKey() (*crypto.PrivKey, error) {
	// Generate a new key pair
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, err
	}

	// Convert private key to bytes
	privBytes, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, err
	}

	// Save private key to a file
	err = os.WriteFile(privateKeyFilename, privBytes, 0644)
	if err != nil {
		return nil, err
	}

	return &priv, nil
}

func readPrivateKey() (*crypto.PrivKey, error) {
	// Read private key from a file
	privBytes, err := os.ReadFile(privateKeyFilename)
	if err != nil {
		return nil, err
	}

	// Unmarshal the private key bytes into a key
	priv, err := crypto.UnmarshalPrivateKey(privBytes)
	if err != nil {
		return nil, err
	}

	return &priv, nil
}
