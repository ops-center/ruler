package ruler

import (
	"flag"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"time"
	"github.com/prometheus/prometheus/promql"
)

// Config is the configuration for the recording rules server.
type Config struct {
	// This is used for template expansion in alerts; must be a valid URL
	ExternalURL string

	// How frequently to evaluate rules by default.
	EvaluationInterval time.Duration
	NumWorkers         int

	// URL of the Alertmanager to send notifications to.
	AlertmanagerURL string

	// Capacity of the queue for notifications to be sent to the Alertmanager.
	NotificationQueueCapacity int
	// HTTP timeout duration when sending notifications to the Alertmanager.
	NotificationTimeout time.Duration
	// Timeout for rule group evaluation, including sending result to ingester
	GroupTimeout time.Duration
}

// AddFlags adds the flags required to config this to the given FlagSet
func (c *Config) AddFlags(f *pflag.FlagSet) {
	f.StringVar(&c.ExternalURL, "ruler.external.url", "", "URL of alerts return path.")
	f.DurationVar(&c.EvaluationInterval, "ruler.evaluation-interval", 15*time.Second, "How frequently to evaluate rules")
	f.IntVar(&c.NumWorkers, "ruler.num-workers", 1, "Number of rule evaluator worker routines in this process")
	f.StringVar(&c.AlertmanagerURL, "ruler.alertmanager-url", "", "URL of the Alertmanager to send notifications to.")
	f.IntVar(&c.NotificationQueueCapacity, "ruler.notification-queue-capacity", 10000, "Capacity of the queue for notifications to be sent to the Alertmanager.")
	f.DurationVar(&c.NotificationTimeout, "ruler.notification-timeout", 10*time.Second, "HTTP timeout duration when sending notifications to the Alertmanager.")
	f.DurationVar(&c.GroupTimeout, "ruler.group-timeout", 10*time.Second, "Timeout for rule group evaluation, including sending result to ingester")
	if flag.Lookup("promql.lookback-delta") == nil {
		flag.DurationVar(&promql.LookbackDelta, "promql.lookback-delta", promql.LookbackDelta, "Time since the last sample after which a time series is considered stale and ignored by expression evaluations.")
	}
}

func (c *Config) Validate() error {
	if c.AlertmanagerURL == "" {
		return errors.New("ruler.alertmanager-url must be non empty")
	}
	return nil
}