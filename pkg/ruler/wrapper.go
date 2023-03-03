package ruler

import "sync"

const (
	UpdateChannelBufferSize = 10000
)

type RuleGetterWrapper struct {
	d           *Distributor
	ruleGetter  RuleClient
	ruleWatcher RuleWatcher

	mtx sync.Mutex

	// TODO: keep prometheus metric for the length?
	newUpdates []RuleGroupsWithInfo
}

func NewRuleGetterWrapper(d *Distributor, rg RuleClient, rw RuleWatcher) (RuleGetter, error) {
	r := &RuleGetterWrapper{
		d:           d,
		ruleGetter:  rg,
		ruleWatcher: rw,
		newUpdates:  []RuleGroupsWithInfo{},
	}
	go r.RunUpdatesCollector()

	return r, nil
}

func (r *RuleGetterWrapper) GetAllRuleGroups() ([]RuleGroupsWithInfo, error) {
	rgs, err := r.ruleGetter.GetAllRuleGroups()
	if err != nil {
		return rgs, err
	}
	if r.d != nil {
		var newRgs []RuleGroupsWithInfo
		for _, rg := range rgs {
			if assigned, err := r.d.IsAssigned(rg.UserID); err == nil && assigned {
				newRgs = append(newRgs, rg)
			}
		}
		return newRgs, nil
	}
	return rgs, err
}

func (r *RuleGetterWrapper) GetAllUpdatedRuleGroups() ([]RuleGroupsWithInfo, error) {
	// slice copy
	var list []RuleGroupsWithInfo
	r.mtx.Lock()
	list = r.newUpdates
	r.newUpdates = []RuleGroupsWithInfo{}
	r.mtx.Unlock()

	if r.d != nil {
		var newRgs []RuleGroupsWithInfo
		for _, rg := range list {
			if assigned, err := r.d.IsAssigned(rg.UserID); err == nil && assigned {
				newRgs = append(newRgs, rg)
			}
		}
		return newRgs, nil
	}
	return list, nil
}

func (r *RuleGetterWrapper) RunUpdatesCollector() {
	ch := make(chan RuleGroupsWithInfo, UpdateChannelBufferSize)
	go r.ruleWatcher.Watch(ch)

	for rg := range ch {
		r.mtx.Lock()
		r.newUpdates = append(r.newUpdates, rg)
		r.mtx.Unlock()
	}
}


