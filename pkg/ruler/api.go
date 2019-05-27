package ruler

import (
	"encoding/json"
	"net/http"

	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"gopkg.in/yaml.v2"
)

// API implements the configs api.
type API struct {
	client RuleClient
	http.Handler
}

// NewAPI creates a new API.
func NewAPI(client RuleClient) *API {
	a := &API{client: client}
	r := mux.NewRouter()
	a.RegisterRoutes(r)
	a.Handler = r
	return a
}

// RegisterRoutes registers the configs API HTTP routes with the provided Router.
func (a *API) RegisterRoutes(r *mux.Router) {
	for _, route := range []struct {
		name, method, path string
		handler            http.HandlerFunc
	}{
		{"get_rules", "GET", "/api/v1/rules", a.getConfig},
		{"set_rules", "POST", "/api/v1/rules", a.setConfig},
	} {
		r.Handle(route.path, route.handler).Methods(route.method).Name(route.name)
	}
}

// getConfig returns the request configuration.
func (a *API) getConfig(w http.ResponseWriter, r *http.Request) {
	userID, err := ExtractUserIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	allRuleGroups, err := a.client.GetUserRuleGroups(userID)
	if err != nil {
		glog.Errorf("for userID %s: get rules: %v", userID, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(allRuleGroups); err != nil {
		glog.Errorf("for userID %s: get rules: %v", userID, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *API) setConfig(w http.ResponseWriter, r *http.Request) {
	userID, err := ExtractUserIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	ruleGroups := &RuleGroupsWithInfo{}
	if err := yaml.NewDecoder(r.Body).Decode(ruleGroups); err != nil {
		glog.Errorf("for userID %s: error decoding json body: %v", userID, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := ruleGroups.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Add client labels for multi-tenancy purpose
	if err := ruleGroups.AddLabelsToQueryExprAddRuleLabel(getLables(userID)); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = a.client.SetRuleGroup(userID, *ruleGroups)
	if err != nil {
		glog.Errorf("for userID %s: error storing rule group: %v", userID, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
