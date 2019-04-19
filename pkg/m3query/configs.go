package m3query

import (
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

const (
	DefaultTimeout = time.Minute * 2
)

type Configs struct {
	// The address where metrics will be sent
	Addr string

	// The CA cert to use for the targets.
	CAFile string

	// The client cert file for the targets.
	CertFile string

	// The client key file for the targets.
	KeyFile string

	// Used to verify the hostname for the targets.
	ServerName string

	// Disable target certificate validation.
	InsecureSkipVerify bool

	// querier
	Timeout       time.Duration
	MaxConcurrent int
}

func NewConfigs() *Configs {
	return &Configs{}
}

func (c *Configs) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&c.Addr, "m3query.url", c.Addr, "The address of m3query where metrics data will be sent")
	fs.StringVar(&c.CAFile, "m3query.ca-cert-file", c.CAFile, "The path of the CA cert to use for the m3query.")
	fs.StringVar(&c.CertFile, "m3query.client-cert-file", c.CertFile, "The path of the client cert to use for communicating with the m3query.")
	fs.StringVar(&c.KeyFile, "m3query.client-key-file", c.KeyFile, "The path of the client key to use for communicating with the m3query.")
	fs.StringVar(&c.ServerName, "m3query.server-name", c.ServerName, "The server name which will be used to verify m3query.")
	fs.BoolVar(&c.InsecureSkipVerify, "m3query.insecure-skip-verify", c.InsecureSkipVerify, "To skip tls verification when communicating with the m3query.")

	fs.DurationVar(&c.Timeout, "querier.timeout", DefaultTimeout, "Specifies the query timeout")
	fs.IntVar(&c.MaxConcurrent, "querier.max-concurrent", 1000, "The maximum number of concurrent queries.")
}

func (c *Configs) Validate() error {
	if c.Addr == "" {
		return errors.New("m3query.url must non-empty")
	}
	if c.Timeout < time.Second*2 {
		return errors.New("querier.timeout must be greater than 5s")
	}
	if c.MaxConcurrent < 1 {
		return errors.New("querier.max-concurrent must be greater than 0")
	}
	return nil
}
