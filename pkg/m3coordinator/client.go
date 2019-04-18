package m3coordinator

import (
	"context"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"net/url"
	prom_config "github.com/prometheus/common/config"
	"github.com/gogo/protobuf/proto"
)

type Writer interface {
	Write(ctx context.Context, samples []prompb.TimeSeries) error
}

type writer struct {
	client   *remote.Client
}

func NewWriter(cfg *Configs) (Writer, error) {
	u, err := url.Parse(cfg.Addr)
	if err != nil {
		return nil, err
	}

	conf := &remote.ClientConfig{
		URL: &prom_config.URL{
			u,
		},
		Timeout: model.Duration(cfg.WriteTimeout),
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
		return nil, err
	}

	return &writer{
		client:client,
	}, nil
}

func (w *writer) Write(ctx context.Context, samples []prompb.TimeSeries) error {
	data, err := buildWriteRequest(samples)
	if err != nil {
		return errors.Wrap(err, "failed to build write request")
	}

	err = w.client.Store(ctx, data)
	if err != nil {
		return errors.Wrap(err, "failed to writes metrics")
	}
	return nil
}

// https://github.com/prometheus/prometheus/blob/84df210c410a0684ec1a05479bfa54458562695e/storage/remote/queue_manager.go#L759
func buildWriteRequest(samples []prompb.TimeSeries) ([]byte, error) {
	req := &prompb.WriteRequest{
		Timeseries: samples,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	compressed := snappy.Encode(nil, data)
	return compressed, nil
}
