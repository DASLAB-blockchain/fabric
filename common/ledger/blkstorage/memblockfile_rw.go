package blkstorage

type memblock struct {
	content []byte
}

type memblockfileWriter struct {
	blocks       *[]*memblock
	currentBlock *memblock
}

func newMemblockfileWriter(mgr *blockfileMgr) (*memblockfileWriter, error) {
	writer := &memblockfileWriter{&mgr.blocks, nil}
	return writer, writer.open()
}

func (w *memblockfileWriter) close() {
	if len(w.currentBlock.content) == 0 {
		return
	}
	*w.blocks = append((*w.blocks), w.currentBlock)
	w.currentBlock = nil
}

func (w *memblockfileWriter) truncateFile(targetSize int) error {
	w.currentBlock.content = w.currentBlock.content[:targetSize]
	return nil
}

func (w *memblockfileWriter) append(b []byte, sync bool) error {
	w.currentBlock.content = append(w.currentBlock.content, b...)
	return nil
}

func (w *memblockfileWriter) open() error {
	w.currentBlock = &memblock{}
	return nil
}
