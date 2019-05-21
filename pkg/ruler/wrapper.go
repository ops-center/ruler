package ruler

type RuleGetterWrapper struct {
	d          *Distributor
	ruleGetter RuleGetter
}

func NewRuleGetterWrapper(d *Distributor, r RuleGetter) (RuleGetter, error) {
	return &RuleGetterWrapper{
		d:          d,
		ruleGetter: r,
	}, nil
}

func (r *RuleGetterWrapper) GetUserRuleGroups(userID string) ([]RuleGroupsWithInfo, error) {
	rgs, err := r.ruleGetter.GetUserRuleGroups(userID)
	if err != nil {
		return nil, err
	}

	if r.d != nil {
		newRgs := []RuleGroupsWithInfo{}
		for _, rg := range rgs {
			if assigned, err := r.d.IsAssigned(rg.UserID); err == nil && assigned {
				newRgs = append(newRgs, rg)
			}
		}
		return newRgs, nil
	}
	return rgs, nil
}

func (r *RuleGetterWrapper) GetRuleGroup(userID string, groupID string) (RuleGroupsWithInfo, error) {
	return r.ruleGetter.GetRuleGroup(userID, groupID)
}

func (r *RuleGetterWrapper) GetAllRuleGroups() (map[string][]RuleGroupsWithInfo, error) {
	rgs, err := r.ruleGetter.GetAllRuleGroups()
	if err != nil {
		return rgs, err
	}
	if r.d != nil {
		newRgs := map[string][]RuleGroupsWithInfo{}
		for uid, rg := range rgs {
			if assigned, err := r.d.IsAssigned(uid); err == nil && assigned {
				newRgs[uid] = rg
			}
		}
		return newRgs, nil
	}
	return rgs, err
}

func (r *RuleGetterWrapper) GetAllRuleGroupsUpdatedOrDeletedAfter(after int64) (map[string][]RuleGroupsWithInfo, error) {
	rgs, err := r.ruleGetter.GetAllRuleGroupsUpdatedOrDeletedAfter(after)
	if err != nil {
		return rgs, err
	}
	if r.d != nil {
		newRgs := map[string][]RuleGroupsWithInfo{}
		for uid, rg := range rgs {
			if assigned, err := r.d.IsAssigned(uid); err == nil && assigned {
				newRgs[uid] = rg
			}
		}
		return newRgs, nil
	}
	return rgs, err
}
