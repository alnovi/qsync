package qsync

import "sync"

type processor struct {
	base *base
	wg   sync.WaitGroup
}

func newProcessor(state *base) *processor {
	return &processor{
		base: state,
	}
}

func (p *processor) Start() {
	p.base.CloseCh = make(chan struct{})
	for queue, _ := range p.base.Matrix {
		newWorker(p.base, queue).Start(&p.wg)
	}
}

func (p *processor) Stop() {
	close(p.base.CloseCh)
	p.wg.Wait()
}
