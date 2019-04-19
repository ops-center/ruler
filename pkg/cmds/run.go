package cmds

import (
	"net/http"

	"github.com/searchlight/ruler/pkg/m3query"

	"github.com/searchlight/ruler/pkg/logger"

	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/searchlight/ruler/pkg/m3coordinator"
	"github.com/searchlight/ruler/pkg/ruler"
	"github.com/spf13/cobra"
)

func NewCmdRun() *cobra.Command {
	rulerCfg := &ruler.Config{}
	m3coordinatorCfg := &m3coordinator.Configs{}
	m3queryCfg := &m3query.Configs{}

	cmd := &cobra.Command{
		Use:               "run",
		Short:             "Launch ruler",
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger.InitLogger()
			logger.Logger.Log("Starting ruler")

			glog.Infof("Starting ruler ...")
			if err := rulerCfg.Validate(); err != nil {
				return err
			}
			if err := m3coordinatorCfg.Validate(); err != nil {
				return err
			}
			if err := m3queryCfg.Validate(); err != nil {
				return err
			}

			writer, err := m3coordinator.NewWriter(m3coordinatorCfg)
			if err != nil {
				return err
			}

			m3qClient, err := m3query.NewClient(m3queryCfg)
			if err != nil {
				return err
			}

			rulr, err := ruler.NewRuler(rulerCfg, m3qClient.GetQueryFunc(), writer)
			if err != nil {
				return err
			}
			defer rulr.Stop()

			rulerClient := ruler.NewInmemRuleStore()
			rulerServer, err := ruler.NewServer(rulerCfg, rulr, rulerClient)
			if err != nil {
				return err
			}
			defer rulerServer.Stop()

			rulerAPI := ruler.NewAPI(rulerClient)

			r := mux.NewRouter()
			rulerAPI.RegisterRoutes(r)
			if err := http.ListenAndServe("0.0.0.0:8443", r); err != nil {
				return err
			}
			return nil
		},
	}

	rulerCfg.AddFlags(cmd.Flags())
	m3coordinatorCfg.AddFlags(cmd.Flags())
	m3queryCfg.AddFlags(cmd.Flags())

	return cmd
}