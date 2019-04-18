package m3query

import (
	"net/url"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	prom_config "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
)

func NewQuerierAndEngine(cfg *Configs, reg prometheus.Registerer) (storage.Queryable, *promql.Engine, error) {
	u, err := url.Parse(cfg.Addr)
	if err != nil {
		return nil, nil, err
	}

	conf := &remote.ClientConfig{
		URL: &prom_config.URL{
			u,
		},
		Timeout: model.Duration(cfg.Timeout),
		HTTPClientConfig: prom_config.HTTPClientConfig{
			TLSConfig: prom_config.TLSConfig{
				CAFile:             cfg.CAFile,
				CertFile:           cfg.CertFile,
				KeyFile:            cfg.KeyFile,
				ServerName:         cfg.ServerName,
				InsecureSkipVerify: cfg.InsecureSkipVerify,
			},
		},
	}

	client, err := remote.NewClient(0, conf)
	if err != nil {
		return nil, nil, err
	}

	querier := remote.QueryableClient(client)

	engine := promql.NewEngine(promql.EngineOpts{
		Logger:        log.NewNopLogger(),
		Timeout:       cfg.Timeout,
		MaxConcurrent: cfg.MaxConcurrent,
		MaxSamples:    cfg.MaxSamples,
		Reg:           reg,
	})
	return querier, engine, nil
}
