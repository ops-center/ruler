package ruler

import (
	"time"

	"github.com/searchlight/ruler/pkg/logger"

	erragg "github.com/appscode/go/util/errors"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/searchlight/prom-query-handler/promquery"
)

type RuleGroupsWithInfo struct {
	UserID             string
	ID                 string `json:"id" yaml:"id"`
	rulefmt.RuleGroups `json:",inline" yaml:",inline"`
	UpdatedAtInUnix    int64
	DeletedAtInUnix    int64
}

func (r *RuleGroupsWithInfo) Validate() error {
	if r.ID == "" {
		return errors.New("rule group id is not provided")
	}
	if errs := r.RuleGroups.Validate(); len(errs) > 0 {
		return erragg.NewAggregate(errs)
	}
	return nil
}

// This will add provided labels as label matchers to query and to rule labels
// In conflict, it will overwrite
func (r *RuleGroupsWithInfo) AddLabelsToQueryExprAddRuleLabel(lbs []labels.Label) error {
	var err error
	for p, g := range r.Groups {
		for q, rg := range r.Groups[p].Rules {
			r.Groups[p].Rules[q].Expr, err = promquery.AddLabelMatchersToQuery(r.Groups[p].Rules[q].Expr, lbs)
			if err != nil {
				return errors.Wrapf(err, "failed to add label matchers to rule expr(%s) for rule group %s", rg.Expr, g.Name)
			}

			lMap := r.Groups[p].Rules[q].Labels
			if lMap == nil {
				lMap = map[string]string{}
			}
			for _, lb := range lbs {
				lMap[lb.Name] = lb.Value
			}
			r.Groups[p].Rules[q].Labels = lMap
		}
	}
	return nil
}

func (r RuleGroupsWithInfo) Parse() ([]ruleGroup, error) {
	groups := []ruleGroup{}

	// TODO: should we set evaluation time
	for _, rg := range r.Groups {
		rls := make([]rules.Rule, 0, len(rg.Rules))
		for _, rl := range rg.Rules {
			expr, err := promql.ParseExpr(rl.Expr)
			if err != nil {
				return nil, err
			}

			if rl.Alert != "" {
				rls = append(rls, rules.NewAlertingRule(
					rl.Alert,
					expr,
					time.Duration(rl.For),
					labels.FromMap(rl.Labels),
					labels.FromMap(rl.Annotations),
					true,
					log.With(logger.Logger, "alert", rl.Alert),
				))
				continue
			}
			rls = append(rls, rules.NewRecordingRule(
				rl.Record,
				expr,
				labels.FromMap(rl.Labels),
			))
		}

		// Group names have to be unique in Prometheus, but only within one rules file.
		groups = append(groups, ruleGroup{
			ruleGroupName: rg.Name,
			rules:         rls,
		})
	}
	return groups, nil
}

type RuleGetter interface {
	GetUserRuleGroups(userID string) ([]RuleGroupsWithInfo, error)
	GetRuleGroup(userID string, groupID string) (RuleGroupsWithInfo, error)
	GetAllRuleGroups() (map[string][]RuleGroupsWithInfo, error)
	GetAllRuleGroupsUpdatedOrDeletedAfter(unixTime int64) (map[string][]RuleGroupsWithInfo, error)
}

type RuleClient interface {
	RuleGetter

	SetRuleGroup(userID string, ruleGroup RuleGroupsWithInfo) error

	DeleteRuleGroup(userID string, groupID string) error
	GetAllDeletedRuleGroups() (map[string][]RuleGroupsWithInfo, error)
	GetAllRuleGroupsDeletedAfter(after int64) (map[string][]RuleGroupsWithInfo, error)
}

type rgWithInfo struct {
	ruleGroups      RuleGroupsWithInfo
	updatedAtInUnix int64
	deletedAtInUnix int64
}

type Inmem struct {
	storage map[string]map[string]RuleGroupsWithInfo
}

func NewInmemRuleStore() RuleClient {
	return &Inmem{
		storage: map[string]map[string]RuleGroupsWithInfo{},
	}
}

func (m *Inmem) GetUserRuleGroups(userID string) ([]RuleGroupsWithInfo, error) {
	rg := []RuleGroupsWithInfo{}
	for _, gs := range m.storage[userID] {
		if gs.DeletedAtInUnix <= 0 {
			rg = append(rg, gs)
		}
	}
	return rg, nil
}

func (m *Inmem) GetRuleGroup(userID string, groupID string) (RuleGroupsWithInfo, error) {
	gs, found := m.storage[userID]
	if found {
		if g, found := gs[groupID]; found && g.DeletedAtInUnix <= 0 {
			return g, nil
		}
	}
	return RuleGroupsWithInfo{}, nil
}

func (m *Inmem) GetAllRuleGroups() (map[string][]RuleGroupsWithInfo, error) {
	rg := map[string][]RuleGroupsWithInfo{}
	for uid, gs := range m.storage {
		rgs := []RuleGroupsWithInfo{}
		for _, v := range gs {
			if v.DeletedAtInUnix <= 0 {
				rgs = append(rgs, v)
			}
		}
		rg[uid] = rgs
	}
	return rg, nil
}

func (m *Inmem) GetAllRuleGroupsUpdatedOrDeletedAfter(after int64) (map[string][]RuleGroupsWithInfo, error) {
	rg := map[string][]RuleGroupsWithInfo{}
	for uid, gs := range m.storage {
		rgs := []RuleGroupsWithInfo{}
		for _, v := range gs {
			if v.DeletedAtInUnix <= 0 && v.UpdatedAtInUnix > after {
				rgs = append(rgs, v)
			}
		}
		rg[uid] = rgs
	}
	return rg, nil
}

func (m *Inmem) SetRuleGroup(userID string, ruleGroup RuleGroupsWithInfo) error {
	if _, found := m.storage[userID]; found {
		m.storage[userID][ruleGroup.ID] = RuleGroupsWithInfo{
			UserID:          userID,
			ID:              ruleGroup.ID,
			RuleGroups:      ruleGroup.RuleGroups,
			UpdatedAtInUnix: time.Now().Unix(),
			DeletedAtInUnix: 0,
		}
	} else {
		m.storage[userID] = map[string]RuleGroupsWithInfo{
			ruleGroup.ID: {
				UserID:          userID,
				ID:              ruleGroup.ID,
				RuleGroups:      ruleGroup.RuleGroups,
				UpdatedAtInUnix: time.Now().Unix(),
				DeletedAtInUnix: 0,
			},
		}
	}
	return nil
}

func (m *Inmem) DeleteRuleGroup(userID string, groupID string) error {
	if gs, found := m.storage[userID]; found {
		if v, found := gs[groupID]; found {
			v.DeletedAtInUnix = time.Now().Unix()
			gs[groupID] = v
		}
	}
	return nil
}

func (m *Inmem) GetAllDeletedRuleGroups() (map[string][]RuleGroupsWithInfo, error) {
	rg := map[string][]RuleGroupsWithInfo{}
	for uid, gs := range m.storage {
		rgs := []RuleGroupsWithInfo{}
		for _, v := range gs {
			if v.DeletedAtInUnix > 0 {
				rgs = append(rgs, v)
			}
		}
		rg[uid] = rgs
	}
	return rg, nil
}

func (m *Inmem) GetAllRuleGroupsDeletedAfter(after int64) (map[string][]RuleGroupsWithInfo, error) {
	rg := map[string][]RuleGroupsWithInfo{}
	for uid, gs := range m.storage {
		rgs := []RuleGroupsWithInfo{}
		for _, v := range gs {
			if v.DeletedAtInUnix > after {
				rgs = append(rgs, v)
			}
		}
		rg[uid] = rgs
	}
	return rg, nil
}
