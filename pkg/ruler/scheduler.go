package ruler

import (
	"context"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"sync"
	"time"

	"github.com/searchlight/ruler/pkg/logger"

	"github.com/go-kit/kit/log/level"
	"github.com/jonboulle/clockwork"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/rules"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/weaveworks/common/instrument"
)

var backoffConfig = util.BackoffConfig{
	// Backoff for loading initial configuration set.
	MinBackoff: 100 * time.Millisecond,
	MaxBackoff: 2 * time.Second,
}

const (
	timeLogFormat = "2006-01-02T15:04:05"
)

var (
	totalUserRules = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "appscode",
		Name:      "rules",
		Help:      "How many user rules the scheduler knows about.",
	})
	ruleUpdates = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "appscode",
		Name:      "scheduler_rule_updates_total",
		Help:      "How many rules updates the scheduler has made.",
	})
	rulesRequestDuration = instrument.NewHistogramCollector(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "appscode",
		Name:      "rules_request_duration_seconds",
		Help:      "Time spent requesting rules.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"operation", "status_code"}))
)

func init() {
	rulesRequestDuration.Register()
	prometheus.MustRegister(totalUserRules)
	prometheus.MustRegister(ruleUpdates)
}

type workItem struct {
	userID        string
	groupID       string
	ruleGroupName string
	group         *group
	scheduled     time.Time
}

// Key implements ScheduledItem
func (w workItem) Key() string {
	return w.userID + ":" + w.groupID + ":" + w.ruleGroupName
}

// Scheduled implements ScheduledItem
func (w workItem) Scheduled() time.Time {
	return w.scheduled
}

// Defer returns a work item with updated rules, rescheduled to a later time.
func (w workItem) Defer(interval time.Duration) workItem {
	return workItem{w.userID, w.groupID, w.ruleGroupName, w.group, w.scheduled.Add(interval)}
}

func (w workItem) String() string {
	return fmt.Sprintf("%s:%s:%s:%d@%s", w.userID, w.groupID, w.ruleGroupName, len(w.group.Rules()), w.scheduled.Format(timeLogFormat))
}

type ruleGroup struct {
	ruleGroupName string
	rules         []rules.Rule
}

type userRules struct {
	ruleList map[string][]ruleGroup // key is groupID
}

type groupFactory func(userID string, groupID string, ruleGroupName string, rls []rules.Rule) (*group, error)

type scheduler struct {
	ruleClient         RuleGetter
	evaluationInterval time.Duration // how often we re-evaluate each rule set
	q                  *SchedulingQueue

	pollInterval time.Duration // how often we check for new rules

	rulesList            map[string]userRules // all rules for all users
	rulesUpdatedAtInUnix int64                // rules updated time

	groupFn groupFactory // function to create a new group
	sync.RWMutex

	stop chan struct{}
	done chan struct{}
}

// newScheduler makes a new scheduler.
func newScheduler(rc RuleGetter, evaluationInterval, pollInterval time.Duration, groupFn groupFactory) scheduler {
	return scheduler{
		ruleClient:         rc,
		evaluationInterval: evaluationInterval,
		pollInterval:       pollInterval,
		q:                  NewSchedulingQueue(clockwork.NewRealClock()),
		rulesList:          map[string]userRules{},
		groupFn:            groupFn,

		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
}

// Run polls the source of rules for changes.
func (s *scheduler) Run() {
	level.Debug(logger.Logger).Log("msg", "scheduler started")
	defer close(s.done)
	// Load initial set of all rules before polling for new ones.
	s.addNewRules(time.Now(), s.loadAllRules())
	ticker := time.NewTicker(s.pollInterval)
	for {
		select {
		case now := <-ticker.C:
			err := s.updateRules(now)
			if err != nil {
				level.Warn(logger.Logger).Log("msg", "scheduler: error updating rules", "err", err)
			}
		case <-s.stop:
			ticker.Stop()
			return
		}
	}
}

func (s *scheduler) Stop() {
	close(s.stop)
	s.q.Close()
	<-s.done
	level.Debug(logger.Logger).Log("msg", "scheduler stopped")
}

// Load the full set of rules from the server, retrying with backoff
// until we can get them.
func (s *scheduler) loadAllRules() map[string][]RuleGroupsWithInfo {
	backoff := util.NewBackoff(context.Background(), backoffConfig)
	for {
		cfgs, err := s.poll(0)
		if err == nil {
			level.Debug(logger.Logger).Log("msg", "scheduler: initial rules load", "num_ruless", len(cfgs))
			return cfgs
		}
		level.Warn(logger.Logger).Log("msg", "scheduler: error fetching all rules, backing off", "err", err)
		backoff.Wait()
	}
}

func (s *scheduler) updateRules(now time.Time) error {
	s.Lock()
	after := s.rulesUpdatedAtInUnix
	s.Unlock()
	rls, err := s.poll(after)
	if err != nil {
		return err
	}

	s.Lock()
	s.rulesUpdatedAtInUnix = getLatestUpdateTime(rls, s.rulesUpdatedAtInUnix)
	s.Unlock()

	s.addNewRules(now, rls)
	return nil
}

// poll all the rules updated or added after given time
// given time in unix format
// if given time is 0, then it will fetch all the rules
func (s *scheduler) poll(after int64) (map[string][]RuleGroupsWithInfo, error) {
	var rules map[string][]RuleGroupsWithInfo
	err := instrument.CollectedRequest(context.Background(), "Rules.GetRules", rulesRequestDuration, instrument.ErrorCode, func(_ context.Context) error {
		var err error
		if after == 0 {
			rules, err = s.ruleClient.GetAllRuleGroups()
		} else {
			rules, err = s.ruleClient.GetAllRuleGroupsUpdatedOrDeletedAfter(after)
		}
		return err
	})
	if err != nil {
		level.Warn(logger.Logger).Log("msg", "scheduler: rules server poll failed", "err", err)
		return nil, err
	}
	return rules, nil
}

// computeNextEvalTime Computes when a user's rules should be next evaluated, based on how far we are through an evaluation cycle
func (s *scheduler) computeNextEvalTime(hasher hash.Hash64, now time.Time, userID string) time.Time {
	intervalNanos := float64(s.evaluationInterval.Nanoseconds())
	// Compute how far we are into the current evaluation cycle
	currentEvalCyclePoint := math.Mod(float64(now.UnixNano()), intervalNanos)

	hasher.Reset()
	hasher.Write([]byte(userID))
	offset := math.Mod(
		// We subtract our current point in the cycle to cause the entries
		// before 'now' to wrap around to the end.
		// We don't want this to come out negative, so we add the interval to it
		float64(hasher.Sum64())+intervalNanos-currentEvalCyclePoint,
		intervalNanos)
	return now.Add(time.Duration(int64(offset)))
}

func (s *scheduler) addNewRules(now time.Time, ruleList map[string][]RuleGroupsWithInfo) {
	// TODO: instrument how many rules we have, both valid & invalid.
	level.Debug(logger.Logger).Log("msg", "adding rules", "num_rules", len(ruleList))
	hasher := fnv.New64a()

	for userID, rules := range ruleList {
		s.addOrRemoveUserRules(now, hasher, userID, rules)
	}

	ruleUpdates.Add(float64(len(ruleList)))
	s.Lock()
	lenRules := len(s.rulesList)
	s.Unlock()
	totalUserRules.Set(float64(lenRules))
}

// TODO: think about the lock, will it block it for long time
// add or update or delete rules
func (s *scheduler) addOrRemoveUserRules(now time.Time, hasher hash.Hash64, userID string, rList []RuleGroupsWithInfo) {
	rulesByGroup := map[string][]ruleGroup{}
	s.Lock()
	defer s.Unlock()
	if usrRules, found := s.rulesList[userID]; found {
		rulesByGroup = usrRules.ruleList
	}

	for _, r := range rList {
		// check whether this rule deleted or not
		if r.DeletedAtInUnix > 0 {
			if _, found := rulesByGroup[r.ID]; found {
				delete(rulesByGroup, r.ID)
			}
			continue
		}

		rls, err := r.Parse()
		if err != nil {
			level.Warn(logger.Logger).Log("msg", "scheduler: invalid rule group", "user_id", userID, "err", err)
			return
		}
		rulesByGroup[r.ID] = rls
	}

	// update or delete map
	if len(rulesByGroup) == 0 {
		if _, found := s.rulesList[userID]; found {
			delete(s.rulesList, userID)
			level.Info(logger.Logger).Log("msg", "scheduler: deleting user", "user_id", userID)
		}
		return
	}

	s.rulesList[userID] = userRules{rulesByGroup}

	level.Info(logger.Logger).Log("msg", "scheduler: updating rules for user", "user_id", userID, "num_groups", len(rulesByGroup))

	evalTime := s.computeNextEvalTime(hasher, now, userID)
	workItems := []workItem{}
	// TODO: Do we need to go through every rule or just updated rule will do
	for groupID, rules := range rulesByGroup {
		level.Debug(logger.Logger).Log("msg", "scheduler: updating rules for user and group", "user_id", userID, "group", groupID, "num_rules", len(rules))

		for _, rg := range rules {
			g, err := s.groupFn(userID, groupID, rg.ruleGroupName, rg.rules)
			if err != nil {
				// XXX: similarly to above if a user has a working configuration and
				// for some reason we cannot create a group for the new one we'll use
				// the last known working configuration
				level.Warn(logger.Logger).Log("msg", "scheduler: failed to create group for user", "user_id", userID, "group", groupID, "err", err)
				return
			}
			workItems = append(workItems, workItem{userID, groupID, rg.ruleGroupName, g, evalTime})
		}
	}

	for _, i := range workItems {
		s.addWorkItem(i)
	}
}

func (s *scheduler) addWorkItem(i workItem) {
	// The queue is keyed by userID+groupID+ruleGroupName, so items for existing userID+groupID+ruleGroupName will be replaced.
	s.q.Enqueue(i)
	level.Debug(logger.Logger).Log("msg", "scheduler: work item added", "item", i)
}

// Get the next scheduled work item, blocking if none.
//
// Call `workItemDone` on the returned item to indicate that it is ready to be
// rescheduled.
func (s *scheduler) nextWorkItem() *workItem {
	level.Debug(logger.Logger).Log("msg", "scheduler: work item requested, pending...")
	// TODO: We are blocking here on the second Dequeue event. Write more
	// tests for the scheduling queue.
	op := s.q.Dequeue()
	if op == nil {
		level.Info(logger.Logger).Log("msg", "queue closed; no more work items")
		return nil
	}
	item := op.(workItem)
	level.Debug(logger.Logger).Log("msg", "scheduler: work item granted", "item", item)
	return &item
}

// workItemDone marks the given item as being ready to be rescheduled.
func (s *scheduler) workItemDone(i workItem) {
	s.Lock()
	usrRules, found := s.rulesList[i.userID]
	var currentRules []ruleGroup
	if found {
		currentRules = usrRules.ruleList[i.groupID]
	}
	s.Unlock()
	if !found || len(currentRules) == 0 {
		level.Debug(logger.Logger).Log("msg", "scheduler: stopping item", "user_id", i.userID, "group_id", i.groupID, "rule_group", i.ruleGroupName, "found", found, "len", len(currentRules))
		return
	}

	// TODO: Add check whether this user belongs to this node

	next := i.Defer(s.evaluationInterval)
	level.Debug(logger.Logger).Log("msg", "scheduler: work item rescheduled", "item", i, "time", next.scheduled.Format(timeLogFormat))
	s.addWorkItem(next)
}

func getLatestUpdateTime(rls map[string][]RuleGroupsWithInfo, cur int64) int64 {
	for _, rs := range rls {
		for _, r := range rs {
			if cur < r.UpdatedAtInUnix {
				cur = r.UpdatedAtInUnix
			}
			if cur < r.DeletedAtInUnix {
				cur = r.DeletedAtInUnix
			}
		}
	}
	return cur
}
