package ruler

import (
	"net/http"


	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
)

const (
	// UserIDHeaderName denotes the UserID the request has been authenticated as
	UserIDHeaderName = "X-AppsCode-UserID"

	// https://github.com/appscode-cloud/m3db-setup/issues/3
	ClientIDLabel  = "byte_builders_client_id"
	ClusterIDLabel = "byte_builders_cluster_id"
	ProductIDLabel = "byte_builders_product_id"
)

func GetRuleGroupID(groupID string, ruleGroupName string) string {
	return groupID + ":" + ruleGroupName
}

func ExtractUserIDFromHTTPRequest(r *http.Request) (string, error) {
	uid := r.Header.Get(UserIDHeaderName)
	if uid == "" {
		return "", errors.New("user id is not provided")
	}
	return uid, nil
}

func SetUserIDInHTTPRequest(userID string, r *http.Request) error {
	uid := r.Header.Get(UserIDHeaderName)
	if uid != "" && uid != userID {
		return errors.New("existing userID didn't match with given userID")
	}
	r.Header.Set(UserIDHeaderName, userID)
	return nil
}

func GetLables(id string) []labels.Label {
	return []labels.Label{
		{
			Name:  ClientIDLabel,
			Value: id,
		},
	}
}
