package cluster

import (
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/pkg/errors"
)

const (
	PodIPEnv        = "POD_IP"
	PodNamespaceEnv = "POD_NAMESPACE"
)

func getAdvertiseAddr(cfg *Config) (string, error) {
	if cfg.AdvertiseAddr != "" {
		return cfg.AdvertiseAddr, nil
	}

	podIP := os.Getenv(PodIPEnv)
	if podIP == "" {
		return "", errors.New("advertise address or POD_IP env is not set")
	}

	bindPort, err := getPort(cfg.BindAddr)
	if err != nil {
		return "", errors.Wrap(err, "invalid listen address")
	}
	return fmt.Sprintf("%s:%d", podIP, bindPort), nil
}

func getPeers(cfg *Config) ([]string, error) {
	if len(cfg.KnownPeers) > 0 {
		return cfg.KnownPeers, nil
	}
	if cfg.HeadlessSvcName != "" {
		ns := os.Getenv(PodNamespaceEnv)
		if ns == "" {
			return nil, errors.New("POD_NAMESPACE env is not set")
		}

		// lookup for headless service will give ip address of all the pods.
		_, srvRecords, err := net.LookupSRV("", "", fmt.Sprintf("%s.%s.svc", cfg.HeadlessSvcName, ns))
		if err != nil {
			return nil, errors.Wrap(err, "failed to get srv records of the headless service")
		}

		// when deploying using statefulset, bindport is same for every pod
		bindPort, err := getPort(cfg.BindAddr)
		if err != nil {
			return nil, errors.Wrap(err, "invalid listen address")
		}
		peers := []string{}
		for _, srv := range srvRecords {
			// The SRV records ends in a "." for the root domain
			addr := srv.Target[:len(srv.Target)-1]
			peers = append(peers, fmt.Sprintf("%s:%d", addr, bindPort))
		}
		return peers, nil
	}
	return nil, errors.New("peers or headless service to discover peers not provided")
}

func getPort(addr string) (int, error) {
	_, bindPortStr, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, err
	}
	bindPort, err := strconv.Atoi(bindPortStr)
	if err != nil {
		return 0, errors.Wrap(err, "invalid address port")
	}
	return bindPort, nil
}
