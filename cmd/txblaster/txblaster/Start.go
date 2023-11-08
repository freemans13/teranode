package txblaster

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bitcoin-sv/ubsv/cmd/txblaster/worker"
	_ "github.com/bitcoin-sv/ubsv/k8sresolver"
	"github.com/bitcoin-sv/ubsv/services/coinbase"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/distributor"
	"github.com/libsv/go-p2p/wire"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc/resolver"
)

const progname = "tx-blaster"

// // Version & commit strings injected at build with -ldflags -X...
var version string
var commit string

var logger utils.Logger

var printProgress uint64

var kafkaProducer sarama.SyncProducer
var kafkaTopic string
var ipv6MulticastConn *net.UDPConn
var ipv6MulticastChan = make(chan worker.Ipv6MulticastMsg)
var totalTransactions atomic.Uint64
var startTime time.Time

func Start() {
	gocore.SetInfo(progname, version, commit)

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger = gocore.Log("txblast", gocore.NewLogLevelFromString(logLevelStr))

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

	flag.Parse()

	prometheusEndpoint, ok := gocore.Config().Get("prometheusEndpoint")
	if ok && prometheusEndpoint != "" {
		logger.Infof("Starting prometheus endpoint on %s", prometheusEndpoint)
		http.Handle(prometheusEndpoint, promhttp.Handler())
	}

	if gocore.Config().GetBool("use_open_tracing", true) {
		logger.Infof("Starting open tracing")
		serviceName, _ := gocore.Config().Get("SERVICE_NAME", "tx-blaster")
		_, closer, err := util.InitGlobalTracer(serviceName)
		if err != nil {
			panic(err)
		}

		defer closer.Close()
	}

	txDistributor, err := distributor.NewDistributor(logger,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)
	if err != nil {
		log.Fatalf("error creating tx distributor: %v", err)
	}

	coinbaseClient, err := coinbase.NewClient(ctx)
	if err != nil {
		log.Fatalf("error creating coinbase tracker client: %v", err)
	}

	if kafka != nil && *kafka != "" {
		logger.Infof("Connecting to kafka at %s", *kafka)
		kafkaURL, err := url.Parse(*kafka)
		if err != nil {
			log.Fatalf("unable to parse kafka url: %v", err)
		}

		clusterAdmin, producer, err := util.ConnectToKafka(kafkaURL)
		if err != nil {
			log.Fatalf("unable to connect to kafka: %v", err)
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
			log.Fatalf("error resolving interface: %v", err)
		}

		addr := &net.UDPAddr{
			IP:   net.ParseIP(*ipv6Address),
			Port: 9999,
			Zone: en0.Name,
		}

		logger.Infof("Starting IPv6 multicast on %s", addr.String())
		ipv6MulticastConn, err = net.DialUDP("udp6", nil, addr)
		if err != nil {
			log.Fatalf("error dialing address: %v", err)
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

	propagationServers := txDistributor.GetPropagationGRPCAddresses()
	if len(propagationServers) == 0 {
		panic("No suitable propagation server connection found")
	}

	logger.Infof("Using %d propagation servers: %+v", len(propagationServers), propagationServers)
	logger.Infof("Starting %d workers", *workers)

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

	for i := 0; i < *workers; i++ {
		workerLogger := gocore.Log(fmt.Sprintf("wrk_%d", i), gocore.NewLogLevelFromString(logLevelStr))
		go startWorker(ctx, workerLogger, i, *rateLimit, coinbaseClient, txDistributor, logIdsFile)
	}

	// start http health check server
	http.Handle("/health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))

	<-ctx.Done()
}

func startWorker(ctx context.Context, logger utils.Logger, workerId int, rateLimit float64,
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
