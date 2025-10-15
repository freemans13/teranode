// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"google.golang.org/protobuf/proto"
)

// txmetaMessageHandler returns a Kafka message handler for transaction metadata operations.
//
// This wrapper provides the context to the actual handler function.
func (u *Server) txmetaMessageHandler(ctx context.Context) func(msg *kafka.KafkaMessage) error {
	return func(msg *kafka.KafkaMessage) error {
		return u.txmetaHandler(ctx, msg)
	}
}

// txmetaHandler processes Kafka messages for transaction metadata cache operations.
//
// Processing errors (ErrProcessing) are logged and the message is marked as completed
// to prevent infinite retry loops on malformed data.
func (u *Server) txmetaHandler(ctx context.Context, msg *kafka.KafkaMessage) error {
	if msg == nil || len(msg.ConsumerMessage.Value) <= chainhash.HashSize {
		return nil
	}

	startTime := time.Now()

	var m kafkamessage.KafkaTxMetaTopicMessage
	if err := proto.Unmarshal(msg.ConsumerMessage.Value, &m); err != nil {
		return err
	}

	hash, err := chainhash.NewHashFromStr(m.TxHash)
	if err != nil {
		return err
	}
	delete := m.Action == kafkamessage.KafkaTxMetaActionType_DELETE
	txMetaBytes := m.Content

	if delete {
		if err := u.DelTxMetaCache(ctx, hash); err != nil {
			prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()

			wrappedErr := errors.NewProcessingError("[txmetaHandler][%s] failed to delete tx meta data", hash, err)
			if errors.Is(err, errors.ErrProcessing) {
				// log the wrapped error, instead of throwing an error on Kafka. The message will never be able to be
				// added to the tx meta cache, so we don't want to keep trying to process it.
				u.logger.Warnf(wrappedErr.Error())

				return nil
			}

			return wrappedErr
		}

		prometheusSubtreeValidationDelTXMetaCacheKafka.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)

		return nil
	}

	if err := u.SetTxMetaCacheFromBytes(ctx, hash[:], txMetaBytes); err != nil {
		prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()

		wrappedErr := errors.NewProcessingError("[txmetaHandler][%s] failed to set tx meta data", hash, err)
		if errors.Is(err, errors.ErrProcessing) {
			// log the wrapped error, instead of throwing an error on Kafka. The message will never be able to be
			// added to the tx meta cache, so we don't want to keep trying to process it.
			u.logger.Debugf(wrappedErr.Error())

			return nil
		}

		return wrappedErr
	}

	prometheusSubtreeValidationSetTXMetaCacheKafka.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)

	return nil
}
