package ruler

import (
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/memberlist"
)

// it implements memberlist.EventDelegate interface.
// Every time a peer join or leave, it gets the event
type eventDelegate struct {
	mtx    sync.Mutex
	do     func()
	logger log.Logger
}

func NewEventDelegate(l log.Logger) (*eventDelegate, error) {
	return &eventDelegate{
		do:     nil,
		logger: l,
	}, nil
}

func (d *eventDelegate) SetDo(f func()) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	d.do = f
}

// NotifyJoin is called if a peer joins the cluster.
func (d *eventDelegate) NotifyJoin(n *memberlist.Node) {
	level.Debug(d.logger).Log("received", "NotifyJoin", "node", n.Name, "addr", n.Address())
	d.mtx.Lock()
	defer d.mtx.Unlock()
	if d.do != nil {
		d.do()
	}
}

// NotifyLeave is called if a peer leaves the cluster.
func (d *eventDelegate) NotifyLeave(n *memberlist.Node) {
	level.Debug(d.logger).Log("received", "NotifyLeave", "node", n.Name, "addr", n.Address())
	d.mtx.Lock()
	defer d.mtx.Unlock()
	if d.do != nil {
		d.do()
	}
}

// NotifyUpdate is called if a cluster peer gets updated.
func (d *eventDelegate) NotifyUpdate(n *memberlist.Node) {
	level.Debug(d.logger).Log("received", "NotifyUpdate", "node", n.Name, "addr", n.Address())
	// Do nothing
}
