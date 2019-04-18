package m3coordinator

import (
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"time"
)

const (
	DefaultTimeout = time.Minute * 3
)

type Configs struct {
	// The address where metrics will be sent
	Addr string

	// Metrics write timeout
	WriteTimeout time.Duration

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
}

func NewConfigs() *Configs {
	return &Configs{}
}

func (c *Configs) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&c.Addr, "m3coordinator.url", c.Addr, "The address of m3coordinator where metrics data will be sent")
	fs.DurationVar(&c.WriteTimeout, "m3coordinator.write-timeout", DefaultTimeout, "Specifies the m3coordinator write timeout")
	fs.StringVar(&c.CAFile, "m3coordinator.ca-cert-file", c.CAFile, "The path of the CA cert to use for the m3coordinator.")
	fs.StringVar(&c.CertFile, "m3coordinator.client-cert-file", c.CertFile, "The path of the client cert to use for communicating with the m3coordinator.")
	fs.StringVar(&c.KeyFile, "m3coordinator.client-key-file", c.KeyFile, "The path of the client key to use for communicating with the m3coordinator.")
	fs.StringVar(&c.ServerName, "m3coordinator.server-name", c.ServerName, "The server name which will be used to verify m3coordinator.")
	fs.BoolVar(&c.InsecureSkipVerify, "m3coordinator.insecure-skip-verify", c.InsecureSkipVerify, "To skip tls verification when communicating with the m3coordinator.")
}

func (c *Configs) Validate() error {
	if c.Addr == "" {
		return errors.New("m3coordinator.url must non-empty")
	}
	if c.WriteTimeout < time.Second*5 {
		return errors.New("m3coordinator.write-timeout must be greater than 5s")
	}
	return nil
}