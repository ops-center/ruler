package m3query

import (
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
	"time"
)

func TestGetQueryRequest(t *testing.T) {
	u, _ := url.Parse("http://heloo.com")
	getQueryRequest(u, "up", time.Now())
}

var (
	data = `{
   "status":"success",
   "data":{
      "resultType":"matrix",
      "result":[
         {
            "metric":{
               "code":"200",
               "handler":"graph",
               "instance":"localhost:9090",
               "job":"prometheus",
               "method":"get"
            },
            "values":[
               [
                  1530220860,
                  "6"
               ],
               [
                  1530220875,
                  "6"
               ],
               [
                  1530220890,
                  "6"
               ]
            ]
         },
         {
            "metric":{
               "code":"200",
               "handler":"label_values",
               "instance":"localhost:9090",
               "job":"prometheus",
               "method":"get"
            },
            "values":[
               [
                  1530220860,
                  "6"
               ],
               [
                  1530220875,
                  "6"
               ],
               [
                  1530220890,
                  "6"
               ]
            ]
         }
      ]
   }
}`
)

func TestM3QueryResponsePromqlVector(t *testing.T) {
	r := &M3QueryResponse{}
	err := json.Unmarshal([]byte(data), r)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(M3QueryResponsePromqlVector(r))
}
