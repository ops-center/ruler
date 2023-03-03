package promquery

import (
	"fmt"
	"testing"


	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
)

func TestAddLabelMatchers(t *testing.T) {
	extraLbs := []labels.Label{
		{
			"client_id",
			"1234",
		},
		{
			"cluster_id",
			"qwert",
		},
	}

	queries := []struct {
		q        string
		extraLbs []labels.Label
		expectQ  string
	}{
		{
			q:        "sum ( http )",
			expectQ:  `sum ( http{client_id="1234",cluster_id="qwert"} )`,
			extraLbs: extraLbs,
		},
		{
			q:        `sum(sort_desc(sum_over_time(ALERTS{alertstate="firing"}[24h]))) by (alertname)`,
			expectQ:  `sum(sort_desc(sum_over_time(ALERTS{client_id="1234",cluster_id="qwert",alertstate="firing"}[24h]))) by (alertname)`,
			extraLbs: extraLbs,
		},
		{
			q:        `delta(cpu_temp_celsius{host="zeus"}[2h])`,
			expectQ:  `delta(cpu_temp_celsius{client_id="1234",cluster_id="qwert",host="zeus"}[2h])`,
			extraLbs: extraLbs,
		},
		{
			q:        `  cpu_temp_celsius  `,
			expectQ:  `  cpu_temp_celsius{client_id="1234",cluster_id="qwert"}  `,
			extraLbs: extraLbs,
		},
		{
			q:        `histogram_quantile(0.9, sum(rate(http_request_duration_seconds_bucket[10m])) by (job, le))`,
			expectQ:  `histogram_quantile(0.9, sum(rate(http_request_duration_seconds_bucket{client_id="1234",cluster_id="qwert"}[10m])) by (job, le))`,
			extraLbs: extraLbs,
		},
		{
			q:        `sum(histogram_quantile(0.9, sum(rate(http_request_duration_seconds_bucket[10m])) by (job, le))) == http`,
			expectQ:  `sum(histogram_quantile(0.9, sum(rate(http_request_duration_seconds_bucket{client_id="1234",cluster_id="qwert"}[10m])) by (job, le))) == http{client_id="1234",cluster_id="qwert"}`,
			extraLbs: extraLbs,
		},
		{
			q:        `http{job="{}"} by (le)`,
			expectQ:  `http{client_id="1234",cluster_id="qwert",job="{}"} by (le)`,
			extraLbs: extraLbs,
		},
		{
			q:        `label_join(up{job="api-server",src1="a",src2="b",src3="c"}, "foo", ",", "src1", "src2", "src3")`,
			expectQ:  `label_join(up{client_id="1234",cluster_id="qwert",job="api-server",src1="a",src2="b",src3="c"}, "foo", ",", "src1", "src2", "src3")`,
			extraLbs: extraLbs,
		},
		{
			q:        `label_join(up, "foo", ",", "src1", "src2", "src3")`,
			expectQ:  `label_join(up{client_id="1234",cluster_id="qwert"}, "foo", ",", "src1", "src2", "src3")`,
			extraLbs: extraLbs,
		},
		{
			q:        ` http_requests_total{status!~"4.."} `,
			expectQ:  ` http_requests_total{client_id="1234",cluster_id="qwert",status!~"4.."} `,
			extraLbs: extraLbs,
		},
		{
			q: `sum(
  instance_memory_limit_bytes - instance_memory_usage_bytes
) by (app, proc) / 1024 / 1024`,

			expectQ: `sum(
  instance_memory_limit_bytes{client_id="1234",cluster_id="qwert"} - instance_memory_usage_bytes{client_id="1234",cluster_id="qwert"}
) by (app, proc) / 1024 / 1024`,
			extraLbs: extraLbs,
		},
		{
			q:        ` topk(3, sum(rate(instance_cpu_time_ns[5m])) by (app, proc)) `,
			expectQ:  ` topk(3, sum(rate(instance_cpu_time_ns{client_id="1234",cluster_id="qwert"}[5m])) by (app, proc)) `,
			extraLbs: extraLbs,
		},
		{
			q:        ` count(instance_cpu_time_ns) by (app) `,
			expectQ:  ` count(instance_cpu_time_ns{client_id="1234",cluster_id="qwert"}) by (app) `,
			extraLbs: extraLbs,
		},
		{
			q:        ` sum by (status_code) (rate(http_requests_total[5m]))`,
			expectQ:  ` sum by (status_code) (rate(http_requests_total{client_id="1234",cluster_id="qwert"}[5m]))`,
			extraLbs: extraLbs,
		},
		{
			q:        ` sum(rate(http_requests_total[5m] offset 5m))`,
			expectQ:  ` sum(rate(http_requests_total{client_id="1234",cluster_id="qwert"}[5m] offset 5m))`,
			extraLbs: extraLbs,
		},
		{
			q:        ` rate(http_requests_total{status_code=~"5.*"}[5m]) > .1 * rate(http_requests_total[5m]) `,
			expectQ:  ` rate(http_requests_total{client_id="1234",cluster_id="qwert",status_code=~"5.*"}[5m]) > .1 * rate(http_requests_total{client_id="1234",cluster_id="qwert"}[5m]) `,
			extraLbs: extraLbs,
		},
		{
			q:        ` 100 * (1 - avg by(instance)(irate(node_cpu{mode="idle"}[5m])))`,
			expectQ:  ` 100 * (1 - avg by(instance)(irate(node_cpu{client_id="1234",cluster_id="qwert",mode="idle"}[5m])))`,
			extraLbs: extraLbs,
		},
		{
			q:        ` node_memory_Active / on (instance) node_memory_MemTotal`,
			expectQ:  ` node_memory_Active{client_id="1234",cluster_id="qwert"} / on (instance) node_memory_MemTotal{client_id="1234",cluster_id="qwert"}`,
			extraLbs: extraLbs,
		},
		{
			q:        ` up > bool 0 `,
			expectQ:  ` up{client_id="1234",cluster_id="qwert"} > bool 0 `,
			extraLbs: extraLbs,
		},
		{
			q:        ` sum(http_requests_total) without (instance) `,
			expectQ:  ` sum(http_requests_total{client_id="1234",cluster_id="qwert"}) without (instance) `,
			extraLbs: extraLbs,
		},
		{
			q:        ` count_values("version", build_version) `,
			expectQ:  ` count_values("version", build_version{client_id="1234",cluster_id="qwert"}) `,
			extraLbs: extraLbs,
		},
		{
			q:        ` {__name__=~"job:.*"}  `,
			expectQ:  ` {client_id="1234",cluster_id="qwert",__name__=~"job:.*"}  `,
			extraLbs: extraLbs,
		},
	}

	for i := 0; i < len(queries); i++ {
		newQ, err := AddLabelMatchersToQuery(queries[i].q, queries[i].extraLbs)
		if assert.Nil(t, err, queries[i].q) {
			assert.Equal(t, queries[i].expectQ, newQ)
		}
	}
}

func TestRemoveDuplicateLabels(t *testing.T) {
	extraLbs := []labels.Label{
		{
			"client_id",
			"1234",
		},
		{
			"cluster_id",
			"qwert",
		},
	}

	queries := []struct {
		q        string
		extraLbs []labels.Label
		expectQ  string
	}{
		{
			q:        "sum ( http )",
			expectQ:  `sum ( http{client_id="1234",cluster_id="qwert"} )`,
			extraLbs: extraLbs,
		},
		{
			q:        `sum(sort_desc(sum_over_time(ALERTS{alertstate="firing"}[24h]))) by (alertname)`,
			expectQ:  `sum(sort_desc(sum_over_time(ALERTS{client_id="1234",cluster_id="qwert",alertstate="firing"}[24h]))) by (alertname)`,
			extraLbs: extraLbs,
		},
		{
			q:        `delta(cpu_temp_celsius{host="zeus"}[2h])`,
			expectQ:  `delta(cpu_temp_celsius{client_id="1234",cluster_id="qwert",host="zeus"}[2h])`,
			extraLbs: extraLbs,
		},
		{
			q:        `  cpu_temp_celsius  `,
			expectQ:  `  cpu_temp_celsius{client_id="1234",cluster_id="qwert"}  `,
			extraLbs: extraLbs,
		},
		{
			q:        `histogram_quantile(0.9, sum(rate(http_request_duration_seconds_bucket[10m])) by (job, le))`,
			expectQ:  `histogram_quantile(0.9, sum(rate(http_request_duration_seconds_bucket{client_id="1234",cluster_id="qwert"}[10m])) by (job, le))`,
			extraLbs: extraLbs,
		},
		{
			q:        `sum(histogram_quantile(0.9, sum(rate(http_request_duration_seconds_bucket[10m])) by (job, le))) == http`,
			expectQ:  `sum(histogram_quantile(0.9, sum(rate(http_request_duration_seconds_bucket{client_id="1234",cluster_id="qwert"}[10m])) by (job, le))) == http{client_id="1234",cluster_id="qwert"}`,
			extraLbs: extraLbs,
		},
		{
			q:        `http{job="{}"} by (le)`,
			expectQ:  `http{client_id="1234",cluster_id="qwert",job="{}"} by (le)`,
			extraLbs: extraLbs,
		},
		{
			q:        `label_join(up{job="api-server",src1="a",src2="b",src3="c"}, "foo", ",", "src1", "src2", "src3")`,
			expectQ:  `label_join(up{client_id="1234",cluster_id="qwert",job="api-server",src1="a",src2="b",src3="c"}, "foo", ",", "src1", "src2", "src3")`,
			extraLbs: extraLbs,
		},
		{
			q:        `label_join(up, "foo", ",", "src1", "src2", "src3")`,
			expectQ:  `label_join(up{client_id="1234",cluster_id="qwert"}, "foo", ",", "src1", "src2", "src3")`,
			extraLbs: extraLbs,
		},
		{
			q:        ` http_requests_total{status!~"4.."} `,
			expectQ:  ` http_requests_total{client_id="1234",cluster_id="qwert",status!~"4.."} `,
			extraLbs: extraLbs,
		},
		{
			q: `sum(
  instance_memory_limit_bytes - instance_memory_usage_bytes
) by (app, proc) / 1024 / 1024`,

			expectQ: `sum(
  instance_memory_limit_bytes{client_id="1234",cluster_id="qwert"} - instance_memory_usage_bytes{client_id="1234",cluster_id="qwert"}
) by (app, proc) / 1024 / 1024`,
			extraLbs: extraLbs,
		},
		{
			q:        ` topk(3, sum(rate(instance_cpu_time_ns[5m])) by (app, proc)) `,
			expectQ:  ` topk(3, sum(rate(instance_cpu_time_ns{client_id="1234",cluster_id="qwert"}[5m])) by (app, proc)) `,
			extraLbs: extraLbs,
		},
		{
			q:        ` count(instance_cpu_time_ns) by (app) `,
			expectQ:  ` count(instance_cpu_time_ns{client_id="1234",cluster_id="qwert"}) by (app) `,
			extraLbs: extraLbs,
		},
		{
			q:        ` sum by (status_code) (rate(http_requests_total[5m]))`,
			expectQ:  ` sum by (status_code) (rate(http_requests_total{client_id="1234",cluster_id="qwert"}[5m]))`,
			extraLbs: extraLbs,
		},
		{
			q:        ` sum(rate(http_requests_total[5m] offset 5m))`,
			expectQ:  ` sum(rate(http_requests_total{client_id="1234",cluster_id="qwert"}[5m] offset 5m))`,
			extraLbs: extraLbs,
		},
		{
			q:        ` rate(http_requests_total{status_code=~"5.*"}[5m]) > .1 * rate(http_requests_total[5m]) `,
			expectQ:  ` rate(http_requests_total{client_id="1234",cluster_id="qwert",status_code=~"5.*"}[5m]) > .1 * rate(http_requests_total{client_id="1234",cluster_id="qwert"}[5m]) `,
			extraLbs: extraLbs,
		},
		{
			q:        ` 100 * (1 - avg by(instance)(irate(node_cpu{mode="idle"}[5m])))`,
			expectQ:  ` 100 * (1 - avg by(instance)(irate(node_cpu{client_id="1234",cluster_id="qwert",mode="idle"}[5m])))`,
			extraLbs: extraLbs,
		},
		{
			q:        ` node_memory_Active / on (instance) node_memory_MemTotal`,
			expectQ:  ` node_memory_Active{client_id="1234",cluster_id="qwert"} / on (instance) node_memory_MemTotal{client_id="1234",cluster_id="qwert"}`,
			extraLbs: extraLbs,
		},
		{
			q:        ` up > bool 0 `,
			expectQ:  ` up{client_id="1234",cluster_id="qwert"} > bool 0 `,
			extraLbs: extraLbs,
		},
		{
			q:        ` sum(http_requests_total) without (instance) `,
			expectQ:  ` sum(http_requests_total{client_id="1234",cluster_id="qwert"}) without (instance) `,
			extraLbs: extraLbs,
		},
		{
			q:        ` count_values("version", build_version) `,
			expectQ:  ` count_values("version", build_version{client_id="1234",cluster_id="qwert"}) `,
			extraLbs: extraLbs,
		},
		{
			q:        ` {__name__=~"job:.*"}  `,
			expectQ:  ` {client_id="1234",cluster_id="qwert",__name__=~"job:.*"}  `,
			extraLbs: extraLbs,
		},
	}

	for i := 0; i < len(queries); i++ {
		var err error
		newQ, _ := AddLabelMatchersToQuery(queries[i].q, queries[i].extraLbs)
		newQ, _ = AddLabelMatchersToQuery(newQ, queries[i].extraLbs)
		newQ, _ = AddLabelMatchersToQuery(newQ, queries[i].extraLbs)
		newQ, err = RemoveDuplicateLabels(newQ, queries[i].extraLbs)
		if assert.Nil(t, err, queries[i].q) {
			assert.Equal(t, queries[i].expectQ, newQ)
		}
	}
}

func TestParse(t *testing.T) {
	/*
		- "sum ( http )"
		- `sum(sort_desc(sum_over_time(ALERTS{alertstate="firing"}[24h]))) by (alertname)`
		-
	*/
	queries := []struct {
		q       string
		expectM []string
	}{
		{
			q: "sum ( http )",
			expectM: []string{
				"http",
			},
		},
		{
			q: `sum(sort_desc(sum_over_time(ALERTS{alertstate="firing"}[24h]))) by (alertname)`,
			expectM: []string{
				"ALERTS",
			},
		},
		{
			q: `delta(cpu_temp_celsius{host="zeus"}[2h])`,
			expectM: []string{
				"cpu_temp_celsius",
			},
		},
		{
			q: `   cpu_temp_celsius  `,
			expectM: []string{
				"cpu_temp_celsius",
			},
		},
		{
			q: `   cpu_temp_celsius  `,
			expectM: []string{
				"cpu_temp_celsius",
			},
		},
		{
			q: `histogram_quantile(0.9, sum(rate(http_request_duration_seconds_bucket[10m])) by (job, le))`,
			expectM: []string{
				"http_request_duration_seconds_bucket",
			},
		},
		{
			q: `sum(histogram_quantile(0.9, sum(rate(http_request_duration_seconds_bucket[10m])) by (job, le))) == http`,
			expectM: []string{
				"http_request_duration_seconds_bucket",
				"http",
			},
		},
		{
			q: `http{job="{}"} by (le)`,
			expectM: []string{
				"http",
			},
		},
		{
			q: `label_join(up{job="api-server",src1="a",src2="b",src3="c"}, "foo", ",", "src1", "src2", "src3")`,
			expectM: []string{
				"up",
			},
		},
		{
			q: `label_join(up, "foo", ",", "src1", "src2", "src3")`,
			expectM: []string{
				"up",
			},
		},
		{
			q: ` http_requests_total{status!~"4.."} `,
			expectM: []string{
				"http_requests_total",
			},
		},
		{
			q: `sum(
  instance_memory_limit_bytes - instance_memory_usage_bytes
) by (app, proc) / 1024 / 1024`,
			expectM: []string{
				"instance_memory_limit_bytes",
				"instance_memory_usage_bytes",
			},
		},
		{
			q: ` topk(3, sum(rate(instance_cpu_time_ns[5m])) by (app, proc)) `,
			expectM: []string{
				"instance_cpu_time_ns",
			},
		},
		{
			q: ` count(instance_cpu_time_ns) by (app) `,
			expectM: []string{
				"instance_cpu_time_ns",
			},
		},
		{
			q: ` sum by (status_code) (rate(http_requests_total[5m]))`,
			expectM: []string{
				"http_requests_total",
			},
		},
		{
			q: ` sum(rate(http_requests_total[5m] offset 5m))`,
			expectM: []string{
				"http_requests_total",
			},
		},
		{
			q: ` rate(http_requests_total{status_code=~"5.*"}[5m]) > .1 * rate(http_requests_total[5m]) `,
			expectM: []string{
				"http_requests_total",
				"http_requests_total",
			},
		},
		{
			q: ` 100 * (1 - avg by(instance)(irate(node_cpu{mode='idle'}[5m])))`,
			expectM: []string{
				"node_cpu",
			},
		},
		{
			q: ` node_memory_Active / on (instance) node_memory_MemTotal`,
			expectM: []string{
				"node_memory_Active",
				"node_memory_MemTotal",
			},
		},
		{
			q: ` up > bool 0 `,
			expectM: []string{
				"up",
			},
		},
		{
			q: ` sum(http_requests_total) without (instance) `,
			expectM: []string{
				"http_requests_total",
			},
		},
		{
			q: ` count_values("version", build_version) `,
			expectM: []string{
				"build_version",
			},
		},
		{
			q: ` {__name__=~"job:.*"}  `,
			expectM: []string{
				"",
			},
		},
	}

	for i := 0; i < len(queries)-1; i++ {
		m, err := parseMetrics(queries[i].q)
		if assert.NoError(t, err) {
			assert.Equal(t, queries[i].expectM, m)
		}
	}
}

func parseMetrics(q string) ([]string, error) {
	m := []string{}
	p := newParser(q)
	itm := p.next()
	for itm.typ != itemEOF {
		nextItm := p.next()
		if itm.typ == itemIdentifier || itm.typ == itemMetricIdentifier {
			if nextItm.typ != itemLeftParen {
				m = append(m, itm.val)
				fmt.Println("pos:", itm.pos)
				fmt.Println("type:", itm.typ.String())
				fmt.Println("val:", itm.val)
				if nextItm.typ == itemLeftBrace {
					/*p.backup()
					fmt.Println("---labels---")
					ls := p.labelSet()
					fmt.Println(ls)
					nextItm = p.peek()
					fmt.Println("---end---")*/
					for nextItm.typ != itemRightBrace && nextItm.typ != itemEOF {
						nextItm = p.next()
					}

				}
				fmt.Println("next type:", nextItm.typ.String())
				fmt.Println("next val:", nextItm.val)
				fmt.Println("--------------")
			}
		}

		if itm.typ > keywordsStart && itm.typ < keywordsEnd {
			if nextItm.typ == itemLeftParen {
				for nextItm.typ != itemRightParen && nextItm.typ != itemEOF {
					nextItm = p.next()
				}
			}
		}

		itm = nextItm
	}
	return m, nil
}
