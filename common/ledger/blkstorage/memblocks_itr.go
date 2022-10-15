package blkstorage

import (
	"github.com/hyperledger/fabric/common/ledger"
	"sync"
)

type blocksItr struct {
	mgr                  *blockfileMgr
	maxBlockNumAvailable uint64
	blockNumToRetrieve   uint64
	closeMarker          bool
	closeMarkerLock      *sync.Mutex
}

func newMemblockItr(mgr *blockfileMgr, startBlockNum uint64) *blocksItr {
	mgr.blkfilesInfoCond.L.Lock()
	defer mgr.blkfilesInfoCond.L.Unlock()
	return &blocksItr{mgr, uint64(len(mgr.blocks) - 1), startBlockNum,
		false, &sync.Mutex{}}
}

func (itr *blocksItr) waitForBlock(blockNum uint64) uint64 {
	itr.mgr.blkfilesInfoCond.L.Lock()
	defer itr.mgr.blkfilesInfoCond.L.Unlock()
	for itr.mgr.blockfilesInfo.lastPersistedBlock < blockNum && !itr.shouldClose() {
		logger.Debugf("Going to wait for newer blocks. maxAvailaBlockNumber=[%d], waitForBlockNum=[%d]",
			itr.mgr.blockfilesInfo.lastPersistedBlock, blockNum)
		itr.mgr.blkfilesInfoCond.Wait()
		logger.Debugf("Came out of wait. maxAvailaBlockNumber=[%d]", itr.mgr.blockfilesInfo.lastPersistedBlock)
	}
	return itr.mgr.blockfilesInfo.lastPersistedBlock
}

func (itr *blocksItr) initStream() error {
	return nil
}

func (itr *blocksItr) shouldClose() bool {
	itr.closeMarkerLock.Lock()
	defer itr.closeMarkerLock.Unlock()
	return itr.closeMarker
}

// Next moves the cursor to next block and returns true iff the iterator is not exhausted
func (itr *blocksItr) Next() (ledger.QueryResult, error) {
	if itr.maxBlockNumAvailable < itr.blockNumToRetrieve {
		itr.maxBlockNumAvailable = itr.waitForBlock(itr.blockNumToRetrieve)
	}
	itr.closeMarkerLock.Lock()
	defer itr.closeMarkerLock.Unlock()
	if itr.closeMarker {
		return nil, nil
	}

	nextBlockBytes, err := itr.mgr.fetchBlockBytes(&fileLocPointer{fileSuffixNum: int(itr.blockNumToRetrieve)})
	if err != nil {
		return nil, err
	}
	itr.blockNumToRetrieve++
	return deserializeBlock(nextBlockBytes)
}

// Close releases any resources held by the iterator
func (itr *blocksItr) Close() {
	itr.mgr.blkfilesInfoCond.L.Lock()
	defer itr.mgr.blkfilesInfoCond.L.Unlock()
	itr.closeMarkerLock.Lock()
	defer itr.closeMarkerLock.Unlock()
	itr.closeMarker = true
	itr.mgr.blkfilesInfoCond.Broadcast()
}
