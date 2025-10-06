// Package kafka provides Kafka consumer and producer implementations for message handling.
package kafka

import (
	"context"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	inmemorykafka "github.com/bitcoin-sv/teranode/util/kafka/in_memory_kafka"
	"github.com/bitcoin-sv/teranode/util/retry"
	"github.com/ordishs/go-utils"
)

const memoryScheme = "memory"

// KafkaMessage wraps sarama.ConsumerMessage to provide additional functionality.
type KafkaMessage struct {
	sarama.ConsumerMessage
}

// KafkaConsumerGroupI defines the interface for Kafka consumer group operations.
type KafkaConsumerGroupI interface {
	// Start begins consuming messages using the provided consumer function and options.
	Start(ctx context.Context, consumerFn func(message *KafkaMessage) error, opts ...ConsumerOption)

	// BrokersURL returns the list of Kafka broker URLs.
	BrokersURL() []string

	// Close gracefully shuts down the consumer group.
	Close() error
}

// KafkaConsumerConfig holds configuration parameters for Kafka consumer.
type KafkaConsumerConfig struct {
	Logger            ulogger.Logger // Logger instance for logging
	URL               *url.URL       // Kafka broker URL
	BrokersURL        []string       // List of Kafka broker URLs
	Topic             string         // Kafka topic to consume from
	Partitions        int            // Number of partitions
	ConsumerGroupID   string         // Consumer group identifier
	AutoCommitEnabled bool           // Whether to auto-commit offsets
	Replay            bool           // Whether to replay messages from the beginning
}

// KafkaConsumerGroup implements KafkaConsumerGroupI interface.
type KafkaConsumerGroup struct {
	Config        KafkaConsumerConfig
	ConsumerGroup sarama.ConsumerGroup
	cancel        atomic.Value
}

// NewKafkaConsumerGroupFromURL creates a new KafkaConsumerGroup from a URL.
func NewKafkaConsumerGroupFromURL(logger ulogger.Logger, url *url.URL, consumerGroupID string, autoCommit bool, kafkaSettings ...*settings.KafkaSettings) (*KafkaConsumerGroup, error) {
	if url == nil {
		return nil, errors.NewConfigurationError("missing kafka url")
	}

	partitions := util.GetQueryParamInt(url, "partitions", 1)

	// Generate a unique group ID for the txmeta Kafka listener, to ensure that each instance of this service will process all txmeta messages.
	// This is necessary because the txmeta messages are used to populate the txmeta cache, which is shared across all instances of this service.
	// groupID := topic + "-" + uuid.New().String()

	// AutoCommitEnabled:
	// txMetaCache : true, we CAN miss.
	// rejected txs : true, we CAN miss.
	// subtree validation : false.
	// block persister : false.
	// block validation: false.

	consumerConfig := KafkaConsumerConfig{
		Logger:            logger,
		URL:               url,
		BrokersURL:        strings.Split(url.Host, ","),
		Topic:             strings.TrimPrefix(url.Path, "/"),
		Partitions:        partitions,
		ConsumerGroupID:   consumerGroupID,
		AutoCommitEnabled: autoCommit,
		// default is start from beginning
		// do not ignore everything that is already queued, this is the case where we start a new consumer group for the first time
		// maybe it shouldn't be called replay because it suggests that the consume will always replay messages from the beginning
		Replay: util.GetQueryParamInt(url, "replay", 1) == 1,
	}

	return NewKafkaConsumerGroup(consumerConfig, kafkaSettings...)
}

// Close gracefully shuts down the Kafka consumer group
func (k *KafkaConsumerGroup) Close() error {
	// Check if the consumer group was properly initialized
	if k == nil || k.Config.Logger == nil {
		return nil
	}

	k.Config.Logger.Infof("[Kafka] %s: initiating shutdown of consumer group for topic %s", k.Config.ConsumerGroupID, k.Config.Topic)

	// cancel the context first to signal all consumers to stop
	if k.cancel.Load() != nil {
		k.Config.Logger.Debugf("[Kafka] %s: canceling context for topic %s", k.Config.ConsumerGroupID, k.Config.Topic)
		k.cancel.Load().(context.CancelFunc)()
	}

	// Then close the consumer group
	if k.ConsumerGroup != nil {
		if err := k.ConsumerGroup.Close(); err != nil {
			k.Config.Logger.Errorf("[Kafka] %s: error closing consumer group for topic %s: %v", k.Config.ConsumerGroupID, k.Config.Topic, err)
			return err
		}

		k.Config.Logger.Infof("[Kafka] %s: successfully closed consumer group for topic %s", k.Config.ConsumerGroupID, k.Config.Topic)
	}

	return nil
}

// NewKafkaConsumerGroup creates a new Kafka consumer group
// We DO NOT read autocommit parameter from the URL because the handler func has specific error handling logic.
func NewKafkaConsumerGroup(cfg KafkaConsumerConfig, kafkaSettings ...*settings.KafkaSettings) (*KafkaConsumerGroup, error) {
	if cfg.URL == nil {
		return nil, errors.NewConfigurationError("kafka URL is not set", nil)
	}

	if cfg.Logger == nil {
		return nil, errors.NewConfigurationError("logger is not set", nil)
	}

	if cfg.ConsumerGroupID == "" {
		return nil, errors.NewConfigurationError("group ID is not set", nil)
	}

	cfg.Logger.Infof("Starting Kafka consumer for topic %s in group %s (concurrency based on partition count)", cfg.Topic, cfg.ConsumerGroupID)

	var consumerGroup sarama.ConsumerGroup

	if cfg.URL.Scheme == memoryScheme {
		// --- Use the in-memory implementation ---
		broker := inmemorykafka.GetSharedBroker() // Get the shared broker instance
		// Create the InMemoryConsumerGroup which implements sarama.ConsumerGroup
		consumerGroup = inmemorykafka.NewInMemoryConsumerGroup(broker, cfg.Topic, cfg.ConsumerGroupID)
		cfg.Logger.Infof("Using in-memory Kafka consumer group")
		// No error expected from creation here, unless topic/group were invalid (checked above)

		return &KafkaConsumerGroup{
			Config:        cfg,
			ConsumerGroup: consumerGroup,
		}, nil
	}

	// --- Use the real Sarama implementation ---
	var err error

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Configure authentication if KafkaSettings are provided
	if len(kafkaSettings) > 0 && kafkaSettings[0] != nil {
		cfg.Logger.Debugf("Configuring Kafka TLS authentication - EnableTLS: %v, SkipVerify: %v, CA: %s, Cert: %s",
			kafkaSettings[0].EnableTLS, kafkaSettings[0].TLSSkipVerify, kafkaSettings[0].TLSCAFile, kafkaSettings[0].TLSCertFile)

		// Validate KafkaSettings before applying them
		if err := ValidateKafkaAuthSettings(kafkaSettings[0]); err != nil {
			return nil, errors.NewConfigurationError("invalid Kafka authentication settings", err)
		}
		if err := configureKafkaAuth(config, kafkaSettings[0]); err != nil {
			return nil, errors.NewConfigurationError("failed to configure Kafka authentication", err)
		}

		cfg.Logger.Debugf("Successfully configured Kafka TLS authentication for consumer group %s", cfg.ConsumerGroupID)
	}

	// https://github.com/IBM/sarama/issues/1689
	// https://github.com/IBM/sarama/pull/1699
	// Default value for config.Consumer.Offsets.AutoCommit.Enable is true.
	if !cfg.AutoCommitEnabled {
		config.Consumer.Offsets.AutoCommit.Enable = false
	}

	if cfg.Replay {
		// defaults to OffsetNewest
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	clusterAdmin, err := sarama.NewClusterAdmin(cfg.BrokersURL, config)
	if err != nil {
		return nil, errors.NewConfigurationError("error while creating cluster admin", err)
	}
	defer func(clusterAdmin sarama.ClusterAdmin) {
		_ = clusterAdmin.Close()
	}(clusterAdmin)

	consumerGroup, err = sarama.NewConsumerGroup(cfg.BrokersURL, cfg.ConsumerGroupID, config)
	if err != nil {
		return nil, errors.NewServiceError("failed to create Kafka consumer group for %s", cfg.Topic, err)
	}

	return &KafkaConsumerGroup{
		Config:        cfg,
		ConsumerGroup: consumerGroup,
	}, nil
}

// ConsumerOption represents an option for configuring the consumer behavior
type ConsumerOption func(*consumerOptions)

type consumerOptions struct {
	withRetryAndMoveOn    bool
	withRetryAndStop      bool
	withLogErrorAndMoveOn bool
	maxRetries            int
	backoffMultiplier     int
	backoffDurationType   time.Duration
	stopFn                func()
}

// WithRetryAndMoveOn configures error behaviour for the consumer function
// After max retries, the error is logged and the message is skipped
func WithRetryAndMoveOn(maxRetries, backoffMultiplier int, backoffDurationType time.Duration) ConsumerOption {
	return func(o *consumerOptions) {
		o.withRetryAndMoveOn = true
		o.withRetryAndStop = false // can't have both options set
		o.maxRetries = maxRetries
		o.backoffMultiplier = backoffMultiplier
		o.backoffDurationType = backoffDurationType
	}
}

// WithRetryAndStop configures error behaviour for the consumer function
// After max retries, the error is logged and message consumption stops
// Use this when you cannot proceed with the next message in the queue
func WithRetryAndStop(maxRetries, backoffMultiplier int, backoffDurationType time.Duration, stopFn func()) ConsumerOption {
	return func(o *consumerOptions) {
		o.withRetryAndMoveOn = false // can't have both options set
		o.withRetryAndStop = true
		o.maxRetries = maxRetries
		o.backoffMultiplier = backoffMultiplier
		o.backoffDurationType = backoffDurationType
		o.stopFn = stopFn
	}
}

// WithLogErrorAndMoveOn configures error behaviour for the consumer function
// When an error occurs, it is logged and the message is skipped without any retries
// Use this for non-critical messages where you want visibility of failures but don't want to block processing
func WithLogErrorAndMoveOn() ConsumerOption {
	return func(o *consumerOptions) {
		o.withLogErrorAndMoveOn = true
		o.withRetryAndMoveOn = false
		o.withRetryAndStop = false
	}
}

func (k *KafkaConsumerGroup) Start(ctx context.Context, consumerFn func(message *KafkaMessage) error, opts ...ConsumerOption) {
	if k == nil {
		return
	}

	options := &consumerOptions{
		withRetryAndMoveOn:    false,
		withRetryAndStop:      false,
		withLogErrorAndMoveOn: false,
		maxRetries:            3,
		backoffMultiplier:     2,
		backoffDurationType:   time.Second,
	}
	for _, opt := range opts {
		opt(options)
	}

	if options.withRetryAndMoveOn {
		originalFn := consumerFn
		consumerFn = func(msg *KafkaMessage) error {
			_, err := retry.Retry(ctx, k.Config.Logger, func() (any, error) {
				return struct{}{}, originalFn(msg)
			},
				retry.WithRetryCount(options.maxRetries),
				retry.WithBackoffMultiplier(options.backoffMultiplier),
				retry.WithBackoffDurationType(options.backoffDurationType),
				retry.WithMessage("[kafka_consumer] retrying processing kafka message..."))

			// if we can't process the message, log the error and skip to the next message
			if err != nil {
				key := ""
				if msg != nil && msg.Key != nil {
					key = utils.ReverseAndHexEncodeSlice(msg.Key)
				}

				k.Config.Logger.Errorf("[kafka_consumer] error processing kafka message on topic %s (key: %s), skipping", k.Config.Topic, key)
			}

			return nil // give up and move on
		}
	}

	if options.withRetryAndStop {
		originalFn := consumerFn
		consumerFn = func(msg *KafkaMessage) error {
			_, err := retry.Retry(ctx, k.Config.Logger, func() (any, error) {
				return struct{}{}, originalFn(msg)
			},
				retry.WithRetryCount(options.maxRetries),
				retry.WithBackoffMultiplier(options.backoffMultiplier),
				retry.WithBackoffDurationType(options.backoffDurationType),
				retry.WithMessage("[kafka_consumer] retrying processing kafka message..."))

			// if we can't process the message, log the error and stop consuming any more messages
			if err != nil {
				if options.stopFn != nil {
					key := ""
					if msg != nil && msg.Key != nil {
						key = utils.ReverseAndHexEncodeSlice(msg.Key)
					}

					k.Config.Logger.Errorf("[kafka_consumer] error processing kafka message on topic %s (key: %s), stopping", k.Config.Topic, key)
					options.stopFn()
				} else {
					c := k.ConsumerGroup
					k.ConsumerGroup = nil

					_ = c.Close()

					panic("error processing kafka message, with no stop function provided")
				}
			}

			return nil
		}
	}

	if options.withLogErrorAndMoveOn {
		originalFn := consumerFn
		consumerFn = func(msg *KafkaMessage) error {
			err := originalFn(msg)

			// if we can't process the message, log the error and skip to the next message
			if err != nil {
				key := ""
				if msg != nil && msg.Key != nil {
					key = utils.ReverseAndHexEncodeSlice(msg.Key)
				}

				k.Config.Logger.Errorf("[kafka_consumer] error processing kafka message on topic %s (key: %s), skipping: %v", k.Config.Topic, key, err)
			}

			return nil // always move on to the next message
		}
	}

	go func() {
		internalCtx, cancel := context.WithCancel(ctx)
		k.cancel.Store(cancel)
		defer cancel()

		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case err := <-k.ConsumerGroup.Errors():
					if err != nil {
						// Don't log context cancellation as an error - it's expected during shutdown
						if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "context canceled") {
							k.Config.Logger.Debugf("Kafka consumer shutdown: %v", err)
						} else {
							k.Config.Logger.Errorf("Kafka consumer error: %v", err)
						}
					}
				}
			}
		}()

		topics := []string{k.Config.Topic}

		// Only spawn one consumer goroutine - Sarama handles partition concurrency internally
		go func() {
			k.Config.Logger.Debugf("[kafka] starting consumer for group %s on topic %s (partition-based concurrency)", k.Config.ConsumerGroupID, topics[0])

			for {
				select {
				case <-internalCtx.Done():
					// Context cancelled, exit goroutine
					return
				default:
					if err := k.ConsumerGroup.Consume(internalCtx, topics, NewKafkaConsumer(k.Config, consumerFn)); err != nil {
						switch {
						case errors.Is(err, sarama.ErrClosedConsumerGroup):
							k.Config.Logger.Infof("[kafka] Consumer for group %s closed", k.Config.ConsumerGroupID)
							return
						case errors.Is(err, context.Canceled):
							k.Config.Logger.Infof("[kafka] Consumer for group %s cancelled", k.Config.ConsumerGroupID)
						default:
							// Consider delay before retry or exit based on error type
							k.Config.Logger.Errorf("Error from consumer: %v", err)
						}
					}
				}
			}
		}()

		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

		go func() {
			<-signals
			cancel()
		}()

		select {
		case <-signals:
			k.Config.Logger.Infof("[kafka] Received signal, shutting down consumers for group %s", k.Config.ConsumerGroupID)
			cancel() // Ensure the context is canceled
		case <-internalCtx.Done():
			k.Config.Logger.Infof("[kafka] Context done, shutting down consumer for %s", k.Config.ConsumerGroupID)
		}

		if k.ConsumerGroup != nil {
			if err := k.ConsumerGroup.Close(); err != nil {
				k.Config.Logger.Errorf("[Kafka] %s: error closing client: %v", k.Config.ConsumerGroupID, err)
			}
		}
	}()
}

func (k *KafkaConsumerGroup) BrokersURL() []string {
	return k.Config.BrokersURL
}

// KafkaConsumer represents a Sarama consumer group consumer
type KafkaConsumer struct {
	consumerClosure func(*KafkaMessage) error
	cfg             KafkaConsumerConfig
}

func NewKafkaConsumer(cfg KafkaConsumerConfig, consumerClosureOrNil func(message *KafkaMessage) error) *KafkaConsumer {
	consumer := &KafkaConsumer{
		consumerClosure: consumerClosureOrNil,
		cfg:             cfg,
	}

	return consumer
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (kc *KafkaConsumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (kc *KafkaConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	if !kc.cfg.AutoCommitEnabled {
		session.Commit()
	}

	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (kc *KafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	const (
		batchSize      = 1000
		commitInterval = time.Minute
	)

	messageProcessedSinceLastCommit := false

	var mu sync.Mutex // Add mutex to protect messageProcessedSinceLastCommit

	// Start a separate goroutine for commit ticker
	if !kc.cfg.AutoCommitEnabled {
		go func() {
			commitTicker := time.NewTicker(commitInterval)
			defer commitTicker.Stop()

			for {
				select {
				case <-session.Context().Done():
					err := session.Context().Err()
					if err != nil {
						kc.cfg.Logger.Debugf("[kafka_consumer] Context canceled in commit ticker (topic: %s): %v",
							kc.cfg.Topic, err)
					}

					return
				case <-commitTicker.C:
					mu.Lock()
					if messageProcessedSinceLastCommit {
						session.Commit()

						messageProcessedSinceLastCommit = false
					}
					mu.Unlock()
				}
			}
		}()
	}

	// Create a buffered channel for messages to reduce context switching
	messages := make(chan *sarama.ConsumerMessage, batchSize)

	// Start a separate goroutine to receive messages
	go func() {
		for message := range claim.Messages() {
			select {
			case messages <- message:
			case <-session.Context().Done():
				// Log when context is canceled in the message forwarding goroutine
				err := session.Context().Err()
				if err != nil {
					kc.cfg.Logger.Debugf("[kafka_consumer] Context canceled in message forwarder (topic: %s, partition: %d): %v",
						claim.Topic(), claim.Partition(), err)
				}

				return
			}
		}
	}()

	for {
		select {
		case <-session.Context().Done():
			err := session.Context().Err()
			// Only log detailed information if it's not a normal shutdown
			if err != nil {
				// Get additional context about the consumer state
				partition := claim.Partition()
				topic := claim.Topic()
				highWatermark := claim.HighWaterMarkOffset()

				kc.cfg.Logger.Infof("[kafka_consumer] Context done for consumer (topic: %s, partition: %d, highWatermark: %d): %v. This is normal during shutdown or rebalancing.",
					topic, partition, highWatermark, err)
			}

			return err

		case message := <-messages:
			if message == nil {
				continue
			}

			// Process batch of messages
			processed := 0
			for ; processed < batchSize; processed++ {
				var err error
				if kc.cfg.AutoCommitEnabled {
					err = kc.handleMessagesWithAutoCommit(message)
				} else {
					err = kc.handleMessageWithManualCommit(session, message)
					if err == nil {
						mu.Lock()
						messageProcessedSinceLastCommit = true
						mu.Unlock()
					}
				}

				if err != nil {
					kc.cfg.Logger.Errorf("[kafka_consumer] failed to process message (topic: %s, partition: %d, offset: %d): %v",
						message.Topic, message.Partition, message.Offset, err)
					return err
				}

				// Try to get next message without blocking
				select {
				case message = <-messages:
					if message == nil {
						break
					}
				default:
					// No more messages immediately available
					goto BatchComplete
				}
			}
		BatchComplete:
		} // nolint:wsl
	}
}

// handleMessageWithManualCommit processes the message and commits the offset only if the processing of the message is successful
func (kc *KafkaConsumer) handleMessageWithManualCommit(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage) error {
	msg := KafkaMessage{*message}
	// kc.cfg.Logger.Infof("Processing message with offset: %v", message.Offset)

	if err := kc.consumerClosure(&msg); err != nil {
		return err
	}

	// kc.logger.Infof("Committing offset: %v", message.Offset)

	// Update the message offset, processing is successful
	// This doesn't commit the offset to the server, it just marks it as processed in memory on the client
	// The commit is done elsewhere
	session.MarkMessage(message, "")

	return nil
}

func (kc *KafkaConsumer) handleMessagesWithAutoCommit(message *sarama.ConsumerMessage) error {
	return kc.consumerClosure(&KafkaMessage{*message})
}
