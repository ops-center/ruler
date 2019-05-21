package ruler

import (
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/searchlight/ruler/pkg/cluster"
	"stathat.com/c/consistent"
)

var (
	distributorMemberSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "appscode",
		Name:      "distributor_member_size",
		Help:      "How many member/node in the distributor hash ring",
	})
)

// Distributor will distribute rule according to userID among the cluster node
type Distributor struct {
	hashRing *consistent.Consistent
	peer     *cluster.Peer
}

func NewDistributor(p *cluster.Peer) (*Distributor, error) {
	d := &Distributor{
		hashRing: consistent.New(),
		peer:     p,
	}
	return d, nil
}

// returns whether this key is assigned to this node
func (d *Distributor) IsAssigned(key string) (bool, error) {
	if d.peer == nil {
		return false, errors.New("peer is nil")
	}
	nodeName, err := d.hashRing.Get(key)
	if err != nil {
		return false, err
	}
	if nodeName == d.peer.Name() {
		return true, nil
	}
	return false, nil
}

// returns assigned node
func (d *Distributor) AssignedNode(key string) (string, error) {
	return d.hashRing.Get(key)
}

// It will reconstruct the hash ring from peer.memberlist
func (d *Distributor) Refresh() {
	if d.peer == nil {
		return
	}

	level.Debug(util.Logger).Log("domain", "distributor", "msg", "refreshing...")
	d.peer.WaitReady()

	var nodeList []string
	for _, nd := range d.peer.Peers() {
		nodeList = append(nodeList, nd.Name)
	}

	d.hashRing.Set(nodeList)
	distributorMemberSize.Set(float64(len(d.hashRing.Members())))
}

func (d *Distributor) MemberNodeList() []string {
	return d.hashRing.Members()
}

func (d *Distributor) HandleRefresh(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			d.Refresh()
		}
	}
}
