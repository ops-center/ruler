package cluster

import "sync"

type BlockManager struct {
	mtx sync.Mutex
	blck bool
	ch chan struct{}
}

func NewBlockManager() *BlockManager {
	b := &BlockManager{
		blck: false,
		ch: make(chan struct{}),
	}
	close(b.ch)
	return b
}

func (b *BlockManager) IsBlocked() bool {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	return b.blck
}

// return whether successfully able to acquire block or not
func (b *BlockManager) Block() bool {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	if b.blck == false {
		return false
	}
	b.blck = true
	b.ch = make(chan struct{})
	return true
}

func (b *BlockManager) UnBlock() {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	b.blck = false
	close(b.ch)
}

func (b *BlockManager) WaitUntilUnBlocked() {
	<-b.ch
}
