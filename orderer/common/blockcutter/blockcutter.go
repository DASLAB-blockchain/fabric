/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blockcutter

import (
	"time"
	
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/flogging"
)

var logger = flogging.MustGetLogger("orderer.common.blockcutter")

type OrdererConfigFetcher interface {
	OrdererConfig() (channelconfig.Orderer, bool)
}

// Receiver defines a sink for the ordered broadcast messages
type Receiver interface {
	// Ordered should be invoked sequentially as messages are ordered
	// Each batch in `messageBatches` will be wrapped into a block.
	// `pending` indicates if there are still messages pending in the receiver.
	Ordered(msg *cb.Envelope) (messageBatches [][]*cb.Envelope, pending bool)

	// Cut returns the current batch and starts a new one
	Cut() []*cb.Envelope

	// Get first txn time stamp
	GetEstimateThpt() int
}

type receiver struct {
	sharedConfigFetcher   OrdererConfigFetcher
	pendingBatch          []*cb.Envelope
	pendingBatchSizeBytes uint32

	PendingBatchStartTime time.Time
	ChannelID             string
	Metrics               *Metrics


	// Field to estimate the throughput
	firstTxnTime		  time.Time
	// Get timestamp list 
	txnTimeList			  []int64
	estimateThpt		  int
	txnCnt				  int
}

// NewReceiverImpl creates a Receiver implementation based on the given configtxorderer manager
func NewReceiverImpl(channelID string, sharedConfigFetcher OrdererConfigFetcher, metrics *Metrics) Receiver {
	return &receiver{
		sharedConfigFetcher: sharedConfigFetcher,
		Metrics:             metrics,
		ChannelID:           channelID,
		firstTxnTime: 		time.Now(),
		estimateThpt: 		0,
		txnCnt: 			0,
		txnTimeList: 		nil,
	}
}

func (r *receiver) GetEstimateThpt() int {
	return r.estimateThpt
}
// Ordered should be invoked sequentially as messages are ordered
//
// messageBatches length: 0, pending: false
//   - impossible, as we have just received a message
//
// messageBatches length: 0, pending: true
//   - no batch is cut and there are messages pending
//
// messageBatches length: 1, pending: false
//   - the message count reaches BatchSize.MaxMessageCount
//
// messageBatches length: 1, pending: true
//   - the current message will cause the pending batch size in bytes to exceed BatchSize.PreferredMaxBytes.
//
// messageBatches length: 2, pending: false
//   - the current message size in bytes exceeds BatchSize.PreferredMaxBytes, therefore isolated in its own batch.
//
// messageBatches length: 2, pending: true
//   - impossible
//
// Note that messageBatches can not be greater than 2.
func (r *receiver) Ordered(msg *cb.Envelope) (messageBatches [][]*cb.Envelope, pending bool) {
	robust_flag := true
	change_thpt_flag := false
	display_time_stamp_flag := true
	if len(r.pendingBatch) == 0 {
		// We are beginning a new batch, mark the time
		r.PendingBatchStartTime = time.Now()
		r.firstTxnTime = r.PendingBatchStartTime
		logger.Warnf("Orderer receives first txn in the block at %v us\n", 
					  r.PendingBatchStartTime.UnixMicro())
	}

	ordererConfig, ok := r.sharedConfigFetcher.OrdererConfig()
	if !ok {
		logger.Panicf("Could not retrieve orderer config to query batch parameters, block cutting is not possible")
	}

	batchSize := ordererConfig.BatchSize()

	messageSizeBytes := messageSizeBytes(msg)
	if messageSizeBytes > batchSize.PreferredMaxBytes {
		logger.Debugf("The current message, with %v bytes, is larger than the preferred batch size of %v bytes and will be isolated.", messageSizeBytes, batchSize.PreferredMaxBytes)

		// cut pending batch, if it has any messages
		if len(r.pendingBatch) > 0 {
			messageBatch := r.Cut()
			messageBatches = append(messageBatches, messageBatch)
		}

		// create new batch with single message
		messageBatches = append(messageBatches, []*cb.Envelope{msg})

		// Record that this batch took no time to fill
		r.Metrics.BlockFillDuration.With("channel", r.ChannelID).Observe(0)

		return
	}

	messageWillOverflowBatchSizeBytes := r.pendingBatchSizeBytes+messageSizeBytes > batchSize.PreferredMaxBytes

	if messageWillOverflowBatchSizeBytes {
		logger.Debugf("The current message, with %v bytes, will overflow the pending batch of %v bytes.", messageSizeBytes, r.pendingBatchSizeBytes)
		logger.Debugf("Pending batch would overflow if current message is added, cutting batch now.")
		messageBatch := r.Cut()
		r.PendingBatchStartTime = time.Now()
		messageBatches = append(messageBatches, messageBatch)
	}

	logger.Debugf("Enqueuing message into batch")
	r.pendingBatch = append(r.pendingBatch, msg)
	r.pendingBatchSizeBytes += messageSizeBytes
	pending = true
	// Enqueue timestamp into the batch
	r.txnTimeList = append(r.txnTimeList, time.Now().UnixMicro())


	if uint32(len(r.pendingBatch)) >= batchSize.MaxMessageCount {
		prevCnt := r.txnCnt
		r.txnCnt += len(r.pendingBatch)
		logger.Debugf("Batch size met, cutting batch")
		if robust_flag {
			delta_time := time.Since(r.firstTxnTime)
			estimate_thpt := len(r.pendingBatch) * 1000000 / int(delta_time.Microseconds())
			r.estimateThpt = estimate_thpt
		}
		// Change batch size based on function
		THR1 := 2000
		THR2 := 3000
		if change_thpt_flag {
			if prevCnt < THR1 && r.txnCnt >= THR1 {
				batchSize.MaxMessageCount = 17
				logger.Warnf("Now has processed %v txns and update blockSize to %d", r.txnCnt, batchSize.MaxMessageCount)	
			} else if prevCnt < THR2 && r.txnCnt >= THR2 {
				batchSize.MaxMessageCount = 10
				logger.Warnf("Now has processed %v txns and update blockSize to %d", r.txnCnt, batchSize.MaxMessageCount)	
			}
		}

		if (display_time_stamp_flag) {
			logger.Warnf("Txn timestamp(us) in this batch: %v", r.txnTimeList)
		}
		messageBatch := r.Cut()
		messageBatches = append(messageBatches, messageBatch)
		pending = false
	}

	return
}

// Cut returns the current batch and starts a new one
func (r *receiver) Cut() []*cb.Envelope {
	if r.pendingBatch != nil {
		r.Metrics.BlockFillDuration.With("channel", r.ChannelID).Observe(time.Since(r.PendingBatchStartTime).Seconds())
	}
	r.PendingBatchStartTime = time.Time{}
	batch := r.pendingBatch
	r.pendingBatch = nil
	r.pendingBatchSizeBytes = 0
	r.txnTimeList = nil
	return batch
}

func messageSizeBytes(message *cb.Envelope) uint32 {
	return uint32(len(message.Payload) + len(message.Signature))
}
