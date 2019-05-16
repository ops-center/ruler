package cluster

import (
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"time"
)

type Config struct {
	// Listen address for cluster
	BindAddr string

	// Explicit address to advertise in cluster
	AdvertiseAddr string

	// Initial peer
	KnownPeers []string

	// Interval for gossip state syncs.
	// Setting this interval lower (more frequent) will increase convergence speeds across
	// larger clusters at the expense of increased bandwidth usage
	PushPullInterval time.Duration

	// Interval between sending gossip messages.
	// By lowering this value (more frequent) gossip messages are propagated across
	// the cluster more quickly at the expense of increased bandwidth
	GossipInterval time.Duration

	// Timeout for establishing a stream connection with a remote node for a full state sync,
	// and for stream read and write operations
	TcpTimeout time.Duration

	// Timeout to wait for an ack from a probed node before assuming it is unhealthy.
	// This should be set to 99-percentile of RTT (round-trip time) on your network
	ProbeTimeout time.Duration

	// Interval between random node probes. Setting this lower (more frequent) will
	// cause the cluster to detect failed nodes more quickly at the expense of increased bandwidth usage
	ProbeInterval time.Duration

	// Use host name as Cluster Node name. If it is not set, then random name will be generated.
	// Node name needs to be unique across the cluster
	UseHostName bool
}


// AddFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) AddFlags(f *pflag.FlagSet) {
	f.StringVar(&cfg.BindAddr, "cluster.listen-address", "0.0.0.0:9094", "Listen address for cluster.")
	f.StringVar(&cfg.AdvertiseAddr, "cluster.advertise-address", "", "Explicit address to advertise in cluster.")
	f.StringArrayVar(&cfg.KnownPeers, "cluster.peer", []string{}, "Initial peers (may be repeated).")
	f.DurationVar(&cfg.GossipInterval, "cluster.gossip-interval", DefaultGossipInterval, "Interval between sending gossip messages. By lowering this value (more frequent) gossip messages are propagated across the cluster more quickly at the expense of increased bandwidth.")
	f.DurationVar(&cfg.PushPullInterval, "cluster.pushpull-interval", DefaultPushPullInterval, "Interval for gossip state syncs. Setting this interval lower (more frequent) will increase convergence speeds across larger clusters at the expense of increased bandwidth usage.")
	f.DurationVar(&cfg.TcpTimeout, "cluster.tcp-timeout", DefaultTcpTimeout, "Timeout for establishing a stream connection with a remote node for a full state sync, and for stream read and write operations.")
	f.DurationVar(&cfg.ProbeTimeout, "cluster.probe-timeout", DefaultProbeTimeout, "Timeout to wait for an ack from a probed node before assuming it is unhealthy. This should be set to 99-percentile of RTT (round-trip time) on your network.")
	f.DurationVar(&cfg.ProbeInterval, "cluster.probe-interval", DefaultProbeInterval, "Interval between random node probes. Setting this lower (more frequent) will cause the cluster to detect failed nodes more quickly at the expense of increased bandwidth usage.")
	f.BoolVar(&cfg.UseHostName, "cluster.use-host-name", true, "Use host name as Cluster Node name. If it is not set, then random name will be generated.Node name needs to be unique across the cluster")
}

func (cfg *Config) Validate() error {
	if cfg.BindAddr == "" {
		return errors.New("--cluster.listen-address must be non empty")
	}
	if cfg.AdvertiseAddr == "" {
		return errors.New("--cluster.advertise-address must be non empty")
	}
	return nil
}