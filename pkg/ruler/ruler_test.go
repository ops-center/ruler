package ruler

import (
	"flag"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/common/user"
)

func newTestRuler(t *testing.T, alertmanagerURL string) *Ruler {
	var cfg Config
	fs := flag.NewFlagSet("test", flag.PanicOnError)
	cfg.AddFlags(fs)
	fs.Parse(nil)
	cfg.AlertmanagerURL.Set(alertmanagerURL)
	cfg.AlertmanagerDiscovery = false

	// TODO: Populate distributor and chunk store arguments to enable
	// other kinds of tests.

	engine := promql.NewEngine(promql.EngineOpts{
		MaxSamples:    1e6,
		MaxConcurrent: 20,
		Timeout:       2 * time.Minute,
	})
	queryable := querier.NewQueryable(nil, nil, nil, 0)
	ruler, err := NewRuler(cfg, engine, queryable, nil)
	if err != nil {
		t.Fatal(err)
	}
	return ruler
}

func TestNotifierSendsUserIDHeader(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1) // We want one request to our test HTTP server.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID, _, err := user.ExtractOrgIDFromHTTPRequest(r)
		assert.NoError(t, err)
		assert.Equal(t, userID, "1")
		wg.Done()
	}))
	defer ts.Close()

	r := newTestRuler(t, ts.URL)
	n, err := r.getOrCreateNotifier("1")
	if err != nil {
		t.Fatal(err)
	}
	for _, not := range r.notifiers {
		defer not.stop()
	}
	// Loop until notifier discovery syncs up
	for len(n.Alertmanagers()) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	n.Send(&notifier.Alert{
		Labels: labels.Labels{labels.Label{Name: "alertname", Value: "testalert"}},
	})

	wg.Wait()
}
