package txblaster

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
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
	"github.com/multiformats/go-multiaddr"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc/resolver"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/pnet"
	"github.com/libp2p/go-libp2p/core/protocol"
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

var usePrivateDht bool = false
var dhtProtocolIdStr string

var sharedKey string

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
	iterations := flag.Int("iterations", -1, "number of iterations to run (default is indefinite)")
	e2e := flag.Bool("e2e", false, "run in e2e mode")

	flag.Parse()

	var ok bool
	dhtProtocolIdStr, ok = gocore.Config().Get("p2p_dht_protocol_id")
	if !ok {
		panic(fmt.Errorf("error getting p2p_dht_protocol_id"))
	}
	sharedKey, ok = gocore.Config().Get("p2p_shared_key")
	if !ok {
		panic(fmt.Errorf("error getting p2p_shared_key"))
	}
	usePrivateDht = gocore.Config().GetBool("p2p_dht_use_private", false)

	if *e2e {
		MIN_BLOCK_HEIGHT_FOR_E2E, _ := gocore.Config().GetInt("min_block_height_for_e2e", 200)

		// Create a channel to signal when the block height condition is met
		blockHeightCh := make(chan struct{})

		// Start a blocking goroutine to check the block height continuously
		go func() {

			for {
				// Define the URL to query block height
				asset_httpAddress, _ := gocore.Config().Get("asset_httpAddress")
				path := "/lastblocks?n=1"
				url := asset_httpAddress + path

				// Send an HTTP GET request to the URL
				resp, err := http.Get(url)
				if err != nil {
					panic("Error: " + err.Error())
				}
				defer resp.Body.Close()

				// Check the response status code
				if resp.StatusCode != http.StatusOK {
					panic(fmt.Sprintf("Error: Unexpected status code %d", resp.StatusCode))
				}

				// Decode the JSON response
				var blocks []struct {
					Height int `json:"height"`
				}

				decoder := json.NewDecoder(resp.Body)
				if err := decoder.Decode(&blocks); err != nil {
					panic("Error: " + err.Error())
				}

				// Extract the height value from the first block (assuming there's only one block in the response)
				if len(blocks) > 0 {
					height := blocks[0].Height
					logger.Infof("Height: %d\n", height)
					// Check if the block height is greater than minHeight
					if minHeight := MIN_BLOCK_HEIGHT_FOR_E2E; height > minHeight {
						logger.Infof("Block height is now %d (greater than %d), signaling to exit.", height, minHeight)
						// Signal to exit the goroutine
						blockHeightCh <- struct{}{}
						return
					}
					logger.Infof("Block height is %d, waiting for it to exceed %d...", height, MIN_BLOCK_HEIGHT_FOR_E2E)
				} else {
					logger.Infof("No blocks found in the response")
				}

				//add a sleep here to control the frequency of block height checks
				time.Sleep(time.Second * 5) // Adjust sleep duration as needed
			}
		}()

		// Block the main program until the block height condition is met
		<-blockHeightCh
	}

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

	var txDistributors []*distributor.Distributor
	nrTxDistributors, _ := gocore.Config().GetInt("txblaster_distributors", 16)

	var err error
	if !*useQuic {
		logger.Debugf("Using %d grpc distributors", nrTxDistributors)
		txDistributors = make([]*distributor.Distributor, nrTxDistributors)
		for i := 0; i < nrTxDistributors; i++ {
			txDistributors[i], err = distributor.NewDistributor(logger,
				distributor.WithBackoffDuration(200*time.Millisecond),
				distributor.WithRetryAttempts(3),
				distributor.WithFailureTolerance(0),
			)
		}
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

		gocore.StartStatsServer(profilerAddr)

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
		// we can get the propagation servers from the first distributor, they are all the same
		propagationServers := txDistributors[0].GetPropagationGRPCAddresses()
		if len(propagationServers) == 0 {
			panic("No suitable propagation server connection found")
		}

		logger.Infof("Using %d propagation servers: %+v", len(propagationServers), propagationServers)
		logger.Infof("Starting %d workers", *workers)
	}
	var logIdsFile chan string
	if *logIds {
		logFile, err := os.OpenFile("/app/data/txblaster.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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

	// var wg sync.WaitGroup
	runIndefinitely := *iterations < 0
	completedCh := make(chan struct{}, *workers)

	staggerWorkersTimeMs, _ := gocore.Config().GetInt("tx_blaster_staggerWorkersTimeMs", 25)
	staggerWorkersTime := time.Duration(staggerWorkersTimeMs) * time.Millisecond

	for i := 0; i < *workers; i++ {
		if *useQuic {
			// create a separate quic distributor for each worker
			txDistributors = make([]*distributor.Distributor, 1)
			txDistributors[0], err = distributor.NewQuicDistributor(logger,
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
		go startWorker(ctx, workerLogger, i, *rateLimit, *iterations, coinbaseClient, txDistributors, logIdsFile, completedCh)

		if !runIndefinitely {
			for i := 0; i < *workers; i++ {
				<-completedCh
			}
			os.Exit(0)
		}
		// stagger worker startup to not overload Coinbase
		time.Sleep(staggerWorkersTime)
	}

	<-ctx.Done()
}

func startWorker(ctx context.Context, logger ulogger.Logger, workerId int, rateLimit float64, iterations int,
	coinbaseClient *coinbase.Client, txDistributors []*distributor.Distributor, logIdsFile chan string, completed chan struct{}) {

	var w *worker.Worker
	var err error

	// Check if the iterations flag was set to a positive value
	runIndefinitely := iterations < 0

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
				iterations,
				coinbaseClient,
				txDistributors,
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
				logger.Errorf("Could not initialise worker %d: %v. Sleeping for 5 seconds", workerId, err)
				time.Sleep(5 * time.Second)
				continue
			}

			// start will only return if an error occurs
			if err = w.Start(ctx); err != nil {
				logger.Errorf("error from worker: %v", err)
			}

			time.Sleep(1 * time.Second)
			if !runIndefinitely {
				logger.Infof("worker %d finished", workerId)
				completed <- struct{}{}
			}
		}
	}
}

func discoverPeers(ctx context.Context, topicName string, h host.Host) {
	var kademliaDHT *dht.IpfsDHT
	var err error
	if usePrivateDht {
		bootstrapAddresses, _ := gocore.Config().GetMulti("p2p_bootstrapAddresses", "|")
		if len(bootstrapAddresses) == 0 {
			panic(fmt.Errorf("bootstrapAddresses not set in config"))
		}
		for _, ba := range bootstrapAddresses {
			bootstrapAddr, err := multiaddr.NewMultiaddr(ba)
			if err != nil {
				panic(err)
			}

			peerInfo, err := peer.AddrInfoFromP2pAddr(bootstrapAddr)
			if err != nil {
				panic(err)
			}

			// Connect to the bootstrap node.
			err = h.Connect(ctx, *peerInfo)
			if err != nil {
				panic(err)
			}
		}
		dhtProtocolID := protocol.ID(dhtProtocolIdStr)
		var options []dht.Option
		options = append(options, dht.ProtocolPrefix(dhtProtocolID))
		options = append(options, dht.Mode(dht.ModeAuto))

		// initialise the DHT
		kademliaDHT, err = dht.New(ctx, h, options...)
		if err != nil {
			panic(err)
		}

		err = kademliaDHT.Bootstrap(ctx)
		if err != nil {
			panic(err)
		}

	} else {
		kademliaDHT = p2p.InitDHT(ctx, h)
	}
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
	var h host.Host
	if usePrivateDht {

		p2pIp, ok := gocore.Config().Get("p2p_ip")
		if !ok {
			panic("p2p_ip not set in config")
		}
		p2pPort, ok := gocore.Config().GetInt("p2p_port")
		if !ok {
			panic("p2p_port not set in config")
		}
		s := ""
		s += fmt.Sprintln("/key/swarm/psk/1.0.0/")
		s += fmt.Sprintln("/base16/")
		s += sharedKey
		psk, err := pnet.DecodeV1PSK(bytes.NewBuffer([]byte(s)))
		if err != nil {
			panic(err)
		}
		h, err = libp2p.New(
			libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/%s/tcp/%d", p2pIp, p2pPort)),
			libp2p.Identity(*pk),
			libp2p.PrivateNetwork(psk),
		)
		if err != nil {
			panic(err)
		}
	} else {
		// Create a new libp2p Host that listens on a random TCP port
		h, err = libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"), libp2p.Identity(*pk))
		if err != nil {
			return nil, err
		}
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
