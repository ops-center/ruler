package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/searchlight/prom-query-handler/promquery"
)

var (
	tokenMap map[string]string
	userCred map[string]string
)

const (
	BEARER_SCHEMA string = "Bearer "
)

func loadToken() {
	tokenMap = map[string]string{
		"1111": "1",
		"2222": "2",
		"3333": "3",
		"4444": "4",
		"5555": "5",
	}
}

func loadUserCred() {
	userCred = map[string]string{
		"1": "1111",
		"2": "2222",
		"3": "3333",
		"4": "4444",
		"5": "5555",
	}
}

// basic auth
// bearer token
func authenticate(r *http.Request) (string, error) {
	user, pass, ok := r.BasicAuth()
	if ok {
		if p, ok := userCred[user]; ok && p == pass {
			return user, nil
		}
		return "", errors.New("invalid username/password")
	}

	token, err := parseBearerToken(r)
	if err != nil {
		return "", err
	}
	if tokenMap != nil {
		if id, ok := tokenMap[token]; ok {
			return id, nil
		}
	}
	return "", errors.New("invalid token")
}

func parseBearerToken(r *http.Request) (string, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return "", errors.New("Authorization header required")
	}
	// Confirm the request is sending Basic Authentication credentials.
	if !strings.HasPrefix(authHeader, BEARER_SCHEMA) {
		return "", errors.New("Authorization requires Bearer scheme")
	}

	// Get the token from the request header
	return authHeader[len(BEARER_SCHEMA):], nil
}

func getLables(id string) []labels.Label {
	return []labels.Label{
		{
			Name:  "client_id",
			Value: id,
		},
	}
}

type Server struct {
	port        string
	m3queryAddr string
	m3queryUrl  *url.URL

	rulerAddr string
	rulerUrl *url.URL
}

func (s *Server) Bootstrap() error {
	loadToken()
	loadUserCred()

	u, err := url.Parse(s.m3queryAddr)
	if err != nil {
		return errors.Wrap(err, "failed to parse m3query url")
	}
	s.m3queryUrl = u


	u, err = url.Parse(s.rulerAddr)
	if err != nil {
		return errors.Wrap(err, "failed to parse ruler url")
	}
	s.rulerUrl = u
	return nil
}

// Serve a reverse proxy
func (s *Server) serveReverseProxyForQuery(w http.ResponseWriter, r *http.Request) {
	// Update the headers to allow for SSL redirection
	r.URL.Host = s.m3queryUrl.Host
	r.URL.Scheme = s.m3queryUrl.Scheme
	r.Header.Set("X-Forwarded-Host", r.Header.Get("Host"))
	r.Host = s.m3queryUrl.Host
	log.Println("query: ", r.URL.Query().Get("query"))

	// wrapper := filter.NewResponseWriterWrapper(w)
	// Note that ServeHttp is non blocking and uses a go routine under the hood
	proxy := httputil.NewSingleHostReverseProxy(s.m3queryUrl)
	proxy.ServeHTTP(w, r)
}

func (s *Server) handleRequestAndRedirect(w http.ResponseWriter, r *http.Request) {
	id, err := authenticate(r)
	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(err.Error()))
		return
	}

	// add label matchers to prometheus query string
	lbs := getLables(id)
	q := r.URL.Query().Get("query")
	if len(q) > 0 {
		log.Printf("From client %s: got query: %s\n", id, q)

		q, err = promquery.AddLabelMatchersToQuery(q, lbs)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}

		qParms := r.URL.Query()
		qParms.Set("query", q)
		r.URL.RawQuery = qParms.Encode()

		log.Printf("From client %s: serving query: %s\n", id, r.URL.Query().Get("query"))
	}
	s.serveReverseProxyForQuery(w, r)
}

func main() {
	srv := &Server{}

	flag.StringVar(&srv.port, "port", "8080", "port number")
	flag.StringVar(&srv.m3queryAddr, "reverse-proxy-url", "http://127.0.0.1:8888", "reverse proxy url")
	flag.Parse()

	err := srv.Bootstrap()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("running reverse proxy server in port %s....\n", srv.port)
	log.Println("redirect url: ", srv.m3queryAddr)
	http.HandleFunc("/", srv.handleRequestAndRedirect)
	if err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%s", srv.port), nil); err != nil {
		log.Fatal(err)
	}
}
