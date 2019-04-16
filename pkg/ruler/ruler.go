package ruler

import (
	native_ctx "context"
	"fmt"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/searchlight/ruler/pkg/m3coordinator"
	"net/http"
	"net/url"
	"sync"
	"time"

	gklog "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	sd_config "github.com/prometheus/prometheus/discovery/config"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/strutil"
	"golang.org/x/net/context"
	"golang.org/x/net/context/ctxhttp"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/weaveworks/common/instrument"
	"github.com/weaveworks/common/user"
)

var (
	evalDuration = instrument.NewHistogramCollectorFromOpts(prometheus.HistogramOpts{
		Namespace: "appscode",
		Name:      "group_evaluation_duration_seconds",
		Help:      "The duration for a rule group to execute.",
		Buckets:   []float64{.1, .25, .5, 1, 2.5, 5, 10, 25},
	})
	rulesProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "appscode",
		Name:      "rules_processed_total",
		Help:      "How many rules have been processed.",
	})
	blockedWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "appscode",
		Name:      "blocked_workers",
		Help:      "How many workers are waiting on an item to be ready.",
	})
	workerIdleTime = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "appscode",
		Name:      "worker_idle_seconds_total",
		Help:      "How long workers have spent waiting for work.",
	})
	evalLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "appscode",
		Name:      "group_evaluation_latency_seconds",
		Help:      "How far behind the target time each rule group executed.",
		Buckets:   []float64{.1, .25, .5, 1, 2.5, 5, 10, 25},
	})
)

func init() {
	evalDuration.Register()
	prometheus.MustRegister(evalLatency)
	prometheus.MustRegister(rulesProcessed)
	prometheus.MustRegister(blockedWorkers)
	prometheus.MustRegister(workerIdleTime)
}

// Ruler evaluates rules.
type Ruler struct {
	engine        *promql.Engine
	queryable     storage.Queryable
	writer        m3coordinator.Writer
	alertURL      *url.URL
	notifierCfg   *config.Config
	queueCapacity int
	groupTimeout  time.Duration
	metrics       *rules.Metrics

	// Per-user notifiers with separate queues.
	notifiersMtx sync.Mutex
	notifiers    map[string]*rulerNotifier
}

// rulerNotifier bundles a notifier.Manager together with an associated
// Alertmanager service discovery manager and handles the lifecycle
// of both actors.
type rulerNotifier struct {
	notifier  *notifier.Manager
	logger    gklog.Logger
}

func newRulerNotifier(o *notifier.Options, l gklog.Logger) *rulerNotifier {
	return &rulerNotifier{
		notifier:  notifier.NewManager(o, l),
		logger:    l,
	}
}

func (rn *rulerNotifier) applyConfig(cfg *config.Config) error {
	if err := rn.notifier.ApplyConfig(cfg); err != nil {
		return err
	}
	return nil
}

func (rn *rulerNotifier) stop() {
	rn.notifier.Stop()
}

// NewRuler creates a new ruler from a distributor and chunk store.
func NewRuler(cfg *Config, engine *promql.Engine, queryable storage.Queryable, w m3coordinator.Writer) (*Ruler, error) {
	ncfg, err := buildNotifierConfig(cfg)
	if err != nil {
		return nil, err
	}

	externalUrl, err := url.Parse(cfg.ExternalURL)
	if err != nil {
		return nil, err
	}

	return &Ruler{
		engine:        engine,
		queryable:     queryable,
		writer:        w,
		alertURL:      externalUrl,
		notifierCfg:   ncfg,
		queueCapacity: cfg.NotificationQueueCapacity,
		notifiers:     map[string]*rulerNotifier{},
		groupTimeout:  cfg.GroupTimeout,
		metrics:       rules.NewGroupMetrics(prometheus.DefaultRegisterer),
	}, nil
}

// Builds a Prometheus config.Config from a ruler.Config with just the required
// options to configure notifications to Alertmanager.
func buildNotifierConfig(rulerConfig *Config) (*config.Config, error) {
	if rulerConfig.AlertmanagerURL == "" {
		return &config.Config{}, nil
	}

	u, err := url.Parse(rulerConfig.AlertmanagerURL)
	if err != nil {
		return nil, err
	}

	sdConfig := sd_config.ServiceDiscoveryConfig{
		StaticConfigs: []*targetgroup.Group{
			{
				Targets: []model.LabelSet{
					{
						model.AddressLabel: model.LabelValue(u.Host),
					},
				},
			},
		},
	}

	amConfig := &config.AlertmanagerConfig{
		Scheme:                 u.Scheme,
		PathPrefix:             u.Path,
		Timeout:                model.Duration(rulerConfig.NotificationTimeout),
		ServiceDiscoveryConfig: sdConfig,
	}

	promConfig := &config.Config{
		AlertingConfig: config.AlertingConfig{
			AlertmanagerConfigs: []*config.AlertmanagerConfig{amConfig},
		},
	}

	if u.User != nil {
		amConfig.HTTPClientConfig = config_util.HTTPClientConfig{
			BasicAuth: &config_util.BasicAuth{
				Username: u.User.Username(),
			},
		}

		if password, isSet := u.User.Password(); isSet {
			amConfig.HTTPClientConfig.BasicAuth.Password = config_util.Secret(password)
		}
	}
	return promConfig, nil
}

func (r *Ruler) newGroup(userID string, groupID string, ruleGroupName string, rls []rules.Rule) (*group, error) {
	appendable := &appendableAppender{writer: r.writer}
	notifier, err := r.getOrCreateNotifier(userID)
	if err != nil {
		return nil, err
	}
	opts := &rules.ManagerOptions{
		Appendable:  appendable,
		QueryFunc:   rules.EngineQueryFunc(r.engine, r.queryable),
		Context:     context.Background(),
		ExternalURL: r.alertURL,
		NotifyFunc:  sendAlerts(notifier, r.alertURL.String()),
		Logger:      util.Logger,
		Metrics:     r.metrics,
	}
	return newGroup(ruleGroupName, rls, appendable, opts), nil
}

// sendAlerts implements a rules.NotifyFunc for a Notifier.
// It filters any non-firing alerts from the input.
//
// Copied from Prometheus's main.go.
func sendAlerts(n *notifier.Manager, externalURL string) rules.NotifyFunc {
	return func(ctx native_ctx.Context, expr string, alerts ...*rules.Alert) {
		var res []*notifier.Alert

		for _, alert := range alerts {
			// Only send actually firing alerts.
			if alert.State == rules.StatePending {
				continue
			}
			a := &notifier.Alert{
				StartsAt:     alert.FiredAt,
				Labels:       alert.Labels,
				Annotations:  alert.Annotations,
				GeneratorURL: externalURL + strutil.TableLinkForExpression(expr),
			}
			if !alert.ResolvedAt.IsZero() {
				a.EndsAt = alert.ResolvedAt
			}
			res = append(res, a)
		}

		if len(alerts) > 0 {
			n.Send(res...)
		}
	}
}

func (r *Ruler) getOrCreateNotifier(userID string) (*notifier.Manager, error) {
	r.notifiersMtx.Lock()
	defer r.notifiersMtx.Unlock()

	n, ok := r.notifiers[userID]
	if ok {
		return n.notifier, nil
	}

	n = newRulerNotifier(&notifier.Options{
		QueueCapacity: r.queueCapacity,
		Do: func(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
			// add userID in header
			if err := SetUserIDInHTTPRequest(userID, req); err != nil {
				return nil, err
			}
			return ctxhttp.Do(ctx, client, req)
		},
	}, util.Logger)

	// This should never fail, unless there's a programming mistake.
	if err := n.applyConfig(r.notifierCfg); err != nil {
		return nil, err
	}

	// TODO: Remove notifiers for stale users. Right now this is a slow leak.
	r.notifiers[userID] = n
	return n.notifier, nil
}

// Evaluate a list of rules in the given context.
func (r *Ruler) Evaluate(userID string, item *workItem) {
	ctx := user.InjectOrgID(context.Background(), userID)
	logger := util.WithContext(ctx, util.Logger)
	level.Debug(logger).Log("msg", "evaluating rules...", "num_rules", len(item.group.Rules()))
	ctx, cancelTimeout := context.WithTimeout(ctx, r.groupTimeout)
	instrument.CollectedRequest(ctx, "Evaluate", evalDuration, nil, func(ctx native_ctx.Context) error {
		if span := opentracing.SpanFromContext(ctx); span != nil {
			span.SetTag("instance", userID)
			span.SetTag("groupName", item.ruleGroupName)
		}
		item.group.Eval(ctx, time.Now())
		return nil
	})
	if err := ctx.Err(); err == nil {
		cancelTimeout() // release resources
	} else {
		level.Warn(logger).Log("msg", "context error", "error", err)
	}

	rulesProcessed.Add(float64(len(item.group.Rules())))
}

// Stop stops the Ruler.
func (r *Ruler) Stop() {
	r.notifiersMtx.Lock()
	defer r.notifiersMtx.Unlock()

	for _, n := range r.notifiers {
		n.stop()
	}
}

// Server is a rules server.
type Server struct {
	scheduler *scheduler
	workers   []worker
}

// NewServer makes a new rule processing server.
func NewServer(cfg *Config, ruler *Ruler, rc RuleClient) (*Server, error) {
	// TODO: Separate configuration for polling interval.
	s := newScheduler(rc, cfg.EvaluationInterval, cfg.EvaluationInterval, ruler.newGroup)
	if cfg.NumWorkers <= 0 {
		return nil, fmt.Errorf("must have at least 1 worker, got %d", cfg.NumWorkers)
	}
	workers := make([]worker, cfg.NumWorkers)
	for i := 0; i < cfg.NumWorkers; i++ {
		workers[i] = newWorker(&s, ruler)
	}
	srv := Server{
		scheduler: &s,
		workers:   workers,
	}
	go srv.run()
	return &srv, nil
}

// Run the server.
func (s *Server) run() {
	go s.scheduler.Run()
	for _, w := range s.workers {
		go w.Run()
	}
	level.Info(util.Logger).Log("msg", "ruler up and running")
}

// Stop the server.
func (s *Server) Stop() {
	for _, w := range s.workers {
		w.Stop()
	}
	s.scheduler.Stop()
}

// Worker does a thing until it's told to stop.
type Worker interface {
	Run()
	Stop()
}

type worker struct {
	scheduler *scheduler
	ruler     *Ruler

	done       chan struct{}
	terminated chan struct{}
}

func newWorker(scheduler *scheduler, ruler *Ruler) worker {
	return worker{
		scheduler:  scheduler,
		ruler:      ruler,
		done:       make(chan struct{}),
		terminated: make(chan struct{}),
	}
}

func (w *worker) Run() {
	defer close(w.terminated)
	for {
		select {
		case <-w.done:
			return
		default:
		}
		waitStart := time.Now()
		blockedWorkers.Inc()
		level.Debug(util.Logger).Log("msg", "waiting for next work item")
		item := w.scheduler.nextWorkItem()
		blockedWorkers.Dec()
		waitElapsed := time.Now().Sub(waitStart)
		if item == nil {
			level.Debug(util.Logger).Log("msg", "queue closed and empty; terminating worker")
			return
		}
		evalLatency.Observe(time.Since(item.scheduled).Seconds())
		workerIdleTime.Add(waitElapsed.Seconds())
		level.Debug(util.Logger).Log("msg", "processing item", "item", item)
		w.ruler.Evaluate(item.userID, item)
		w.scheduler.workItemDone(*item)
		level.Debug(util.Logger).Log("msg", "item handed back to queue", "item", item)
	}
}

func (w *worker) Stop() {
	close(w.done)
	<-w.terminated
}
