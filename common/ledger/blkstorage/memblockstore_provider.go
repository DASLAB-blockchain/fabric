package blkstorage

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/dataformat"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/common/metrics"
)

var logger = flogging.MustGetLogger("blkstorage")

// IndexableAttr represents an indexable attribute
type IndexableAttr string

// constants for indexable attributes
const (
	IndexableAttrBlockNum        = IndexableAttr("BlockNum")
	IndexableAttrBlockHash       = IndexableAttr("BlockHash")
	IndexableAttrTxID            = IndexableAttr("TxID")
	IndexableAttrBlockNumTranNum = IndexableAttr("BlockNumTranNum")
)

// IndexConfig - a configuration that includes a list of attributes that should be indexed
type IndexConfig struct {
	AttrsToIndex []IndexableAttr
}

// SnapshotInfo captures some of the details about the snapshot
type SnapshotInfo struct {
	LastBlockNum      uint64
	LastBlockHash     []byte
	PreviousBlockHash []byte
}

// Contains returns true iff the supplied parameter is present in the IndexConfig.AttrsToIndex
func (c *IndexConfig) Contains(indexableAttr IndexableAttr) bool {
	for _, a := range c.AttrsToIndex {
		if a == indexableAttr {
			return true
		}
	}
	return false
}

// BlockStoreProvider provides handle to block storage - this is not thread-safe
type BlockStoreProvider struct {
	conf            *Conf
	indexConfig     *IndexConfig
	leveldbProvider *leveldbhelper.Provider
	stats           *stats
	stores          map[string]*BlockStore
}

// NewProvider constructs a filesystem based block store provider
func NewProvider(conf *Conf, indexConfig *IndexConfig, metricsProvider metrics.Provider) (*BlockStoreProvider, error) {
	dbConf := &leveldbhelper.Conf{
		DBPath:         conf.getIndexDir(),
		ExpectedFormat: dataFormatVersion(indexConfig),
	}

	p, err := leveldbhelper.NewProvider(dbConf)
	if err != nil {
		return nil, err
	}

	stats := newStats(metricsProvider)
	return &BlockStoreProvider{conf, indexConfig, p, stats, make(map[string]*BlockStore)}, nil
}

// Open opens a block store for given ledgerid.
// If a blockstore is not existing, this method creates one
// This method should be invoked only once for a particular ledgerid
func (p *BlockStoreProvider) Open(ledgerid string) (*BlockStore, error) {
	indexStoreHandle := p.leveldbProvider.GetDBHandle(ledgerid)
	if store, ok := p.stores[ledgerid]; ok {
		//do something here
		return store, nil
	}

	newstore, err := newBlockStore(ledgerid, p.conf, p.indexConfig, indexStoreHandle, p.stats)
	if err == nil {
		p.stores[ledgerid] = newstore
	}
	return newstore, err
}

// ImportFromSnapshot initializes blockstore from a previously generated snapshot
// Any failure during bootstrapping the blockstore may leave the partial loaded data
// on disk. The consumer, such as peer is expected to keep track of failures and cleanup the
// data explicitly.
func (p *BlockStoreProvider) ImportFromSnapshot(
	ledgerID string,
	snapshotDir string,
	snapshotInfo *SnapshotInfo,
) error {
	panic("Not supported")
}

// Exists tells whether the BlockStore with given id exists
func (p *BlockStoreProvider) Exists(ledgerid string) (bool, error) {
	if _, ok := p.stores[ledgerid]; ok {
		return true, nil
	}
	return false, nil
}

// Drop drops blockstore data (block index and blocks directory) for the given ledgerid (channelID).
// It is not an error if the channel does not exist.
// This function is not error safe. If this function returns an error or a crash takes place, it is highly likely
// that the data for this ledger is left in an inconsistent state. Opening the ledger again or reusing the previously
// opened ledger can show unknown behavior.
func (p *BlockStoreProvider) Drop(ledgerid string) error {
	exists, err := p.Exists(ledgerid)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if err := p.leveldbProvider.Drop(ledgerid); err != nil {
		return err
	}
	delete(p.stores, ledgerid)
	return nil
}

// List lists the ids of the existing ledgers
func (p *BlockStoreProvider) List() ([]string, error) {
	keys := []string{}
	for key, _ := range p.stores {
		keys = append(keys, key)
	}
	return keys, nil
}

// Close closes the BlockStoreProvider
func (p *BlockStoreProvider) Close() {
	p.leveldbProvider.Close()
}

func dataFormatVersion(indexConfig *IndexConfig) string {
	// in version 2.0 we merged three indexable into one `IndexableAttrTxID`
	if indexConfig.Contains(IndexableAttrTxID) {
		return dataformat.CurrentFormat
	}
	return dataformat.PreviousFormat
}
