package blkstorage

import (
	"bytes"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/internal/fileutil"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"math"
	"sync"
	"sync/atomic"
)

const (
	blockfilePrefix                   = "blockfile_"
	bootstrappingSnapshotInfoFile     = "bootstrappingSnapshot.info"
	bootstrappingSnapshotInfoTempFile = "bootstrappingSnapshotTemp.info"
)

var blkMgrInfoKey = []byte("blkMgrInfo")

type blockfileMgr struct {
	conf                      *Conf
	db                        *leveldbhelper.DBHandle
	index                     *blockIndex
	blockfilesInfo            *blockfilesInfo
	bootstrappingSnapshotInfo *BootstrappingSnapshotInfo
	blkfilesInfoCond          *sync.Cond
	currentFileWriter         *memblockfileWriter
	bcInfo                    atomic.Value
	blocks                    []*memblock
}

func newBlockfileMgr(id string, conf *Conf, indexConfig *IndexConfig, indexStore *leveldbhelper.DBHandle) (*blockfileMgr, error) {
	logger.Debugf("newBlockfileMgr() initializing memory-based block storage for ledger: %s ", id)
	mgr := &blockfileMgr{conf: conf, db: indexStore}

	blockfilesInfo, err := mgr.loadBlkfilesInfo()
	if err != nil {
		panic(fmt.Sprintf("Could not get block file info for current block file from db: %s", err))
	}
	if blockfilesInfo == nil {
		logger.Info(`Creating Empty blockfilesInfo`)
		if blockfilesInfo, err = mgr.createEmptyBlkfilesInfo(); err != nil {
			panic(fmt.Sprintf("Could not build blockfilesInfo info from block files: %s", err))
		}
		logger.Debugf("Info constructed by scanning the blocks dir = %s", spew.Sdump(blockfilesInfo))
	}
	err = mgr.saveBlkfilesInfo(blockfilesInfo, true)

	currentFileWriter, err := newMemblockfileWriter(mgr)
	if err != nil {
		panic(fmt.Sprintf("Could not open writer to current file: %s", err))
	}
	if mgr.index, err = newBlockIndex(indexConfig, indexStore); err != nil {
		panic(fmt.Sprintf("error in block index: %s", err))
	}

	mgr.blockfilesInfo = blockfilesInfo

	mgr.bootstrappingSnapshotInfo = nil
	mgr.currentFileWriter = currentFileWriter
	mgr.blkfilesInfoCond = sync.NewCond(&sync.Mutex{})

	if err := mgr.syncIndex(); err != nil {
		return nil, err
	}

	bcInfo := &common.BlockchainInfo{}
	mgr.bcInfo.Store(bcInfo)
	return mgr, nil
}

func (mgr *blockfileMgr) loadBlkfilesInfo() (*blockfilesInfo, error) {
	var b []byte
	var err error
	if b, err = mgr.db.Get(blkMgrInfoKey); b == nil || err != nil {
		return nil, err
	}
	i := &blockfilesInfo{}
	if err = i.unmarshal(b); err != nil {
		return nil, err
	}
	logger.Debugf("loaded blockfilesInfo:%s", i)
	return i, nil
}

func (mgr *blockfileMgr) saveBlkfilesInfo(i *blockfilesInfo, sync bool) error {
	b, err := i.marshal()
	if err != nil {
		return err
	}
	if err = mgr.db.Put(blkMgrInfoKey, b, sync); err != nil {
		return err
	}
	return nil
}

func (mgr *blockfileMgr) close() {
	mgr.currentFileWriter.close()
}

func (mgr *blockfileMgr) iindex() *blockIndex {
	return mgr.index
}

func (mgr *blockfileMgr) moveToNextFile() {
	blkfilesInfo := &blockfilesInfo{
		latestFileNumber:   mgr.blockfilesInfo.latestFileNumber + 1,
		latestFileSize:     0,
		lastPersistedBlock: mgr.blockfilesInfo.lastPersistedBlock,
	}

	nextFileWriter, err := newMemblockfileWriter(mgr)
	if err != nil {
		panic(fmt.Sprintf("Could not open writer to next file: %s", err))
	}
	mgr.currentFileWriter.close()

	err = mgr.saveBlkfilesInfo(blkfilesInfo, true)
	if err != nil {
		panic(fmt.Sprintf("Could not save next block file info to db: %s", err))
	}
	mgr.currentFileWriter = nextFileWriter
	mgr.updateBlockfilesInfo(blkfilesInfo)
}

func (mgr *blockfileMgr) getBlockchainInfo() *common.BlockchainInfo {
	return mgr.bcInfo.Load().(*common.BlockchainInfo)
}

func (mgr *blockfileMgr) updateBlockfilesInfo(blkfilesInfo *blockfilesInfo) {
	mgr.blkfilesInfoCond.L.Lock()
	defer mgr.blkfilesInfoCond.L.Unlock()
	mgr.blockfilesInfo = blkfilesInfo
	logger.Debugf("Broadcasting about update blockfilesInfo: %s", blkfilesInfo)
	mgr.blkfilesInfoCond.Broadcast()
}

func (mgr *blockfileMgr) updateBlockchainInfo(latestBlockHash []byte, latestBlock *common.Block) {
	currentBCInfo := mgr.getBlockchainInfo()
	newBCInfo := &common.BlockchainInfo{
		Height:                    currentBCInfo.Height + 1,
		CurrentBlockHash:          latestBlockHash,
		PreviousBlockHash:         latestBlock.Header.PreviousHash,
		BootstrappingSnapshotInfo: currentBCInfo.BootstrappingSnapshotInfo,
	}

	mgr.bcInfo.Store(newBCInfo)
}

func (mgr *blockfileMgr) addBlock(block *common.Block) error {
	bcInfo := mgr.getBlockchainInfo()
	if block.Header.Number != bcInfo.Height {
		return errors.Errorf(
			"block number should have been %d but was %d",
			mgr.getBlockchainInfo().Height, block.Header.Number,
		)
	}

	// Add the previous hash check - Though, not essential but may not be a bad idea to
	// verify the field `block.Header.PreviousHash` present in the block.
	// This check is a simple bytes comparison and hence does not cause any observable performance penalty
	// and may help in detecting a rare scenario if there is any bug in the ordering service.
	if !bytes.Equal(block.Header.PreviousHash, bcInfo.CurrentBlockHash) {
		return errors.Errorf(
			"unexpected Previous block hash. Expected PreviousHash = [%x], PreviousHash referred in the latest block= [%x]",
			bcInfo.CurrentBlockHash, block.Header.PreviousHash,
		)
	}
	blockBytes, info, err := serializeBlock(block)
	if err != nil {
		return errors.WithMessage(err, "error serializing block")
	}
	blockHash := protoutil.BlockHeaderHash(block.Header)
	// Get the location / offset where each transaction starts in the block and where the block ends
	txOffsets := info.txOffsets
	currentOffset := mgr.blockfilesInfo.latestFileSize

	blockBytesLen := len(blockBytes)
	blockBytesEncodedLen := proto.EncodeVarint(uint64(blockBytesLen))
	totalBytesToAppend := blockBytesLen + len(blockBytesEncodedLen)

	// Determine if we need to start a new file since the size of this block
	// exceeds the amount of space left in the current file
	//if currentOffset+totalBytesToAppend > mgr.conf.maxBlockfileSize {

	currentOffset = 0
	//}
	// append blockBytesEncodedLen to the file
	err = mgr.currentFileWriter.append(blockBytesEncodedLen, false)
	if err == nil {
		// append the actual block bytes to the file
		err = mgr.currentFileWriter.append(blockBytes, true)
	}
	if err != nil {
		truncateErr := mgr.currentFileWriter.truncateFile(mgr.blockfilesInfo.latestFileSize)
		if truncateErr != nil {
			panic(fmt.Sprintf("Could not truncate current file to known size after an error during block append: %s", err))
		}
		return errors.WithMessage(err, "error appending block to file")
	}

	// Update the blockfilesInfo with the results of adding the new block
	currentBlkfilesInfo := mgr.blockfilesInfo
	newBlkfilesInfo := &blockfilesInfo{
		latestFileNumber:   currentBlkfilesInfo.latestFileNumber,
		latestFileSize:     currentBlkfilesInfo.latestFileSize + totalBytesToAppend,
		noBlockFiles:       false,
		lastPersistedBlock: block.Header.Number,
	}
	// save the blockfilesInfo in the database
	if err = mgr.saveBlkfilesInfo(newBlkfilesInfo, false); err != nil {
		truncateErr := mgr.currentFileWriter.truncateFile(currentBlkfilesInfo.latestFileSize)
		if truncateErr != nil {
			panic(fmt.Sprintf("Error in truncating current file to known size after an error in saving blockfiles info: %s", err))
		}
		return errors.WithMessage(err, "error saving blockfiles file info to db")
	}

	// Index block file location pointer updated with file suffex and offset for the new block
	blockFLP := &fileLocPointer{fileSuffixNum: newBlkfilesInfo.latestFileNumber}
	blockFLP.offset = currentOffset
	// shift the txoffset because we prepend length of bytes before block bytes
	for _, txOffset := range txOffsets {
		txOffset.loc.offset += len(blockBytesEncodedLen)
	}
	// save the index in the database
	if err = mgr.index.indexBlock(&blockIdxInfo{
		blockNum: block.Header.Number, blockHash: blockHash,
		flp: blockFLP, txOffsets: txOffsets, metadata: block.Metadata,
	}); err != nil {
		return err
	}

	// update the blockfilesInfo (for storage) and the blockchain info (for APIs) in the manager
	mgr.updateBlockfilesInfo(newBlkfilesInfo)
	mgr.updateBlockchainInfo(blockHash, block)

	// Always use a new filewriter for each new block
	mgr.moveToNextFile()

	return nil
}

func (mgr *blockfileMgr) firstPossibleBlockNumberInBlockFiles() uint64 {
	return 0
}

func (mgr *blockfileMgr) createEmptyBlkfilesInfo() (*blockfilesInfo, error) {
	logger.Debugf("constructing BlockfilesInfo")
	blkfilesInfo := &blockfilesInfo{
		latestFileNumber:   0,
		latestFileSize:     0,
		noBlockFiles:       true,
		lastPersistedBlock: 0,
	}
	logger.Debugf("No block file found")
	return blkfilesInfo, nil
}

func (mgr *blockfileMgr) syncIndex() error {
	nextIndexableBlock := uint64(0)
	lastBlockIndexed, err := mgr.index.getLastBlockIndexed()
	if err != nil {
		if err != errIndexSavePointKeyNotPresent {
			return err
		}
	} else {
		nextIndexableBlock = lastBlockIndexed + 1
	}

	if mgr.blockfilesInfo.noBlockFiles {
		logger.Debug("No block files present. This happens when there has not been any blocks added to the ledger yet")
		return nil
	}

	if nextIndexableBlock == mgr.blockfilesInfo.lastPersistedBlock+1 {
		logger.Debug("Both the block files and indices are in sync.")
		return nil
	}

	return nil
}

func (mgr *blockfileMgr) retrieveBlockByHash(blockHash []byte) (*common.Block, error) {
	logger.Debugf("retrieveBlockByHash() - blockHash = [%#v]", blockHash)
	loc, err := mgr.index.getBlockLocByHash(blockHash)
	if err != nil {
		return nil, err
	}
	return mgr.fetchBlock(loc)
}

func (mgr *blockfileMgr) retrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	logger.Debugf("retrieveBlockByNumber() - blockNum = [%d]", blockNum)

	// interpret math.MaxUint64 as a request for last block
	if blockNum == math.MaxUint64 {
		blockNum = mgr.getBlockchainInfo().Height - 1
	}
	if blockNum < mgr.firstPossibleBlockNumberInBlockFiles() {
		return nil, errors.Errorf(
			"cannot serve block [%d]. The ledger is bootstrapped from a snapshot. First available block = [%d]",
			blockNum, mgr.firstPossibleBlockNumberInBlockFiles(),
		)
	}
	loc, err := mgr.index.getBlockLocByBlockNum(blockNum)
	if err != nil {
		return nil, err
	}
	return mgr.fetchBlock(loc)
}

func (mgr *blockfileMgr) retrieveBlockByTxID(txID string) (*common.Block, error) {
	logger.Debugf("retrieveBlockByTxID() - txID = [%s]", txID)
	loc, err := mgr.index.getBlockLocByTxID(txID)
	if err == errNilValue {
		return nil, errors.Errorf(
			"details for the TXID [%s] not available. Ledger bootstrapped from a snapshot. First available block = [%d]",
			txID, mgr.firstPossibleBlockNumberInBlockFiles())
	}
	if err != nil {
		return nil, err
	}
	return mgr.fetchBlock(loc)
}

func (mgr *blockfileMgr) retrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, uint64, error) {
	logger.Debugf("retrieveTxValidationCodeByTxID() - txID = [%s]", txID)
	validationCode, blkNum, err := mgr.index.getTxValidationCodeByTxID(txID)
	if err == errNilValue {
		return peer.TxValidationCode(-1), 0, errors.Errorf(
			"details for the TXID [%s] not available. Ledger bootstrapped from a snapshot. First available block = [%d]",
			txID, mgr.firstPossibleBlockNumberInBlockFiles())
	}
	return validationCode, blkNum, err
}

func (mgr *blockfileMgr) retrieveBlockHeaderByNumber(blockNum uint64) (*common.BlockHeader, error) {
	logger.Debugf("retrieveBlockHeaderByNumber() - blockNum = [%d]", blockNum)
	if blockNum < mgr.firstPossibleBlockNumberInBlockFiles() {
		return nil, errors.Errorf(
			"cannot serve block [%d]. The ledger is bootstrapped from a snapshot. First available block = [%d]",
			blockNum, mgr.firstPossibleBlockNumberInBlockFiles(),
		)
	}
	loc, err := mgr.index.getBlockLocByBlockNum(blockNum)
	if err != nil {
		return nil, err
	}
	blockBytes, err := mgr.fetchBlockBytes(loc)
	if err != nil {
		return nil, err
	}
	info, err := extractSerializedBlockInfo(blockBytes)
	if err != nil {
		return nil, err
	}
	return info.blockHeader, nil
}

func (mgr *blockfileMgr) retrieveBlocks(startNum uint64) (*blocksItr, error) {
	if startNum < mgr.firstPossibleBlockNumberInBlockFiles() {
		return nil, errors.Errorf(
			"cannot serve block [%d]. The ledger is bootstrapped from a snapshot. First available block = [%d]",
			startNum, mgr.firstPossibleBlockNumberInBlockFiles(),
		)
	}
	return newMemblockItr(mgr, startNum), nil
}

func (mgr *blockfileMgr) txIDExists(txID string) (bool, error) {
	return mgr.index.txIDExists(txID)
}

func (mgr *blockfileMgr) retrieveTransactionByID(txID string) (*common.Envelope, error) {
	logger.Debugf("retrieveTransactionByID() - txId = [%s]", txID)
	loc, err := mgr.index.getTxLoc(txID)
	if err == errNilValue {
		return nil, errors.Errorf(
			"details for the TXID [%s] not available. Ledger bootstrapped from a snapshot. First available block = [%d]",
			txID, mgr.firstPossibleBlockNumberInBlockFiles())
	}
	if err != nil {
		return nil, err
	}
	return mgr.fetchTransactionEnvelope(loc)
}

func (mgr *blockfileMgr) retrieveTransactionByBlockNumTranNum(blockNum uint64, tranNum uint64) (*common.Envelope, error) {
	logger.Debugf("retrieveTransactionByBlockNumTranNum() - blockNum = [%d], tranNum = [%d]", blockNum, tranNum)
	if blockNum < mgr.firstPossibleBlockNumberInBlockFiles() {
		return nil, errors.Errorf(
			"cannot serve block [%d]. The ledger is bootstrapped from a snapshot. First available block = [%d]",
			blockNum, mgr.firstPossibleBlockNumberInBlockFiles(),
		)
	}
	loc, err := mgr.index.getTXLocByBlockNumTranNum(blockNum, tranNum)
	if err != nil {
		return nil, err
	}
	return mgr.fetchTransactionEnvelope(loc)
}

func (mgr *blockfileMgr) fetchBlock(lp *fileLocPointer) (*common.Block, error) {
	blockBytes, err := mgr.fetchBlockBytes(lp)
	if err != nil {
		return nil, err
	}
	block, err := deserializeBlock(blockBytes)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func (mgr *blockfileMgr) fetchTransactionEnvelope(lp *fileLocPointer) (*common.Envelope, error) {
	logger.Debugf("Entering fetchTransactionEnvelope() %v\n", lp)
	var err error
	var txEnvelopeBytes []byte
	if txEnvelopeBytes, err = mgr.fetchRawBytes(lp); err != nil {
		return nil, err
	}
	_, n := proto.DecodeVarint(txEnvelopeBytes)
	return protoutil.GetEnvelopeFromBlock(txEnvelopeBytes[n:])
}

func (mgr *blockfileMgr) fetchBlockBytes(lp *fileLocPointer) ([]byte, error) {
	buffer := mgr.blocks[lp.fileSuffixNum].content
	peekBytes := buffer[0:8]
	length, n := proto.DecodeVarint(peekBytes)
	if n == 0 {
		// proto.DecodeVarint did not consume any byte at all which means that the bytes
		// representing the size of the block are partial bytes
		panic(errors.Errorf("Error in decoding varint bytes [%#v]", peekBytes))
	}
	bytesExpected := int64(n) + int64(length)
	if bytesExpected != int64(len(buffer)) {
		logger.Debugf("At least [%d] bytes expected. Remaining bytes = [%d]. Returning with error [%s]",
			bytesExpected, len(buffer), ErrUnexpectedEndOfBlockfile)
		return nil, ErrUnexpectedEndOfBlockfile
	}
	// skip the bytes representing the block size
	return buffer[n:], nil
}

func (mgr *blockfileMgr) fetchRawBytes(lp *fileLocPointer) ([]byte, error) {
	buffer := mgr.blocks[lp.fileSuffixNum].content
	return buffer[lp.offset : lp.offset+lp.bytesLength], nil
}

// scanForLastCompleteBlock scan a given block file and detects the last offset in the file
// after which there may lie a block partially written (towards the end of the file in a crash scenario).
func scanForLastCompleteBlock(rootDir string, fileNum int, startingOffset int64) ([]byte, int64, int, error) {
	// scan the passed file number suffix starting from the passed offset to find the last completed block
	numBlocks := 0
	var lastBlockBytes []byte
	blockStream, errOpen := newBlockfileStream(rootDir, fileNum, startingOffset)
	if errOpen != nil {
		return nil, 0, 0, errOpen
	}
	defer blockStream.close()
	var errRead error
	var blockBytes []byte
	for {
		blockBytes, errRead = blockStream.nextBlockBytes()
		if blockBytes == nil || errRead != nil {
			break
		}
		lastBlockBytes = blockBytes
		numBlocks++
	}
	if errRead == ErrUnexpectedEndOfBlockfile {
		logger.Debugf(`Error:%s
		The error may happen if a crash has happened during block appending.
		Resetting error to nil and returning current offset as a last complete block's end offset`, errRead)
		errRead = nil
	}
	logger.Debugf("scanForLastCompleteBlock(): last complete block ends at offset=[%d]", blockStream.currentOffset)
	return lastBlockBytes, blockStream.currentOffset, numBlocks, errRead
}

// blockfilesInfo maintains the summary about the blockfiles
type blockfilesInfo struct {
	latestFileNumber   int
	latestFileSize     int
	noBlockFiles       bool
	lastPersistedBlock uint64
}

func (i *blockfilesInfo) marshal() ([]byte, error) {
	buffer := proto.NewBuffer([]byte{})
	var err error
	if err = buffer.EncodeVarint(uint64(i.latestFileNumber)); err != nil {
		return nil, errors.Wrapf(err, "error encoding the latestFileNumber [%d]", i.latestFileNumber)
	}
	if err = buffer.EncodeVarint(uint64(i.latestFileSize)); err != nil {
		return nil, errors.Wrapf(err, "error encoding the latestFileSize [%d]", i.latestFileSize)
	}
	if err = buffer.EncodeVarint(i.lastPersistedBlock); err != nil {
		return nil, errors.Wrapf(err, "error encoding the lastPersistedBlock [%d]", i.lastPersistedBlock)
	}
	var noBlockFilesMarker uint64
	if i.noBlockFiles {
		noBlockFilesMarker = 1
	}
	if err = buffer.EncodeVarint(noBlockFilesMarker); err != nil {
		return nil, errors.Wrapf(err, "error encoding noBlockFiles [%d]", noBlockFilesMarker)
	}
	return buffer.Bytes(), nil
}

func (i *blockfilesInfo) unmarshal(b []byte) error {
	buffer := proto.NewBuffer(b)
	var val uint64
	var noBlockFilesMarker uint64
	var err error

	if val, err = buffer.DecodeVarint(); err != nil {
		return err
	}
	i.latestFileNumber = int(val)

	if val, err = buffer.DecodeVarint(); err != nil {
		return err
	}
	i.latestFileSize = int(val)

	if val, err = buffer.DecodeVarint(); err != nil {
		return err
	}
	i.lastPersistedBlock = val
	if noBlockFilesMarker, err = buffer.DecodeVarint(); err != nil {
		return err
	}
	i.noBlockFiles = noBlockFilesMarker == 1
	return nil
}

func (i *blockfilesInfo) String() string {
	return fmt.Sprintf("latestFileNumber=[%d], latestFileSize=[%d], noBlockFiles=[%t], lastPersistedBlock=[%d]",
		i.latestFileNumber, i.latestFileSize, i.noBlockFiles, i.lastPersistedBlock)
}

func deriveBlockfilePath(rootDir string, suffixNum int) string {
	return rootDir + "/" + blockfilePrefix + fmt.Sprintf("%06d", suffixNum)
}

func bootstrapFromSnapshottedTxIDs(
	ledgerID string,
	snapshotDir string,
	snapshotInfo *SnapshotInfo,
	conf *Conf,
	indexStore *leveldbhelper.DBHandle,
) error {
	rootDir := conf.getLedgerBlockDir(ledgerID)
	isEmpty, err := fileutil.CreateDirIfMissing(rootDir)
	if err != nil {
		return err
	}
	if !isEmpty {
		return errors.Errorf("dir %s not empty", rootDir)
	}

	bsi := &BootstrappingSnapshotInfo{
		LastBlockNum:      snapshotInfo.LastBlockNum,
		LastBlockHash:     snapshotInfo.LastBlockHash,
		PreviousBlockHash: snapshotInfo.PreviousBlockHash,
	}

	bsiBytes, err := proto.Marshal(bsi)
	if err != nil {
		return err
	}

	if err := fileutil.CreateAndSyncFileAtomically(
		rootDir,
		bootstrappingSnapshotInfoTempFile,
		bootstrappingSnapshotInfoFile,
		bsiBytes,
		0o644,
	); err != nil {
		return err
	}
	if err := fileutil.SyncDir(rootDir); err != nil {
		return err
	}
	if err := importTxIDsFromSnapshot(snapshotDir, snapshotInfo.LastBlockNum, indexStore); err != nil {
		return err
	}
	return nil
}
