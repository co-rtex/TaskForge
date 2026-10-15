// Command probe is the part of `make topology-smoke` that runs INSIDE the
// smoke's Docker network, in a container of its own beside the api and worker
// containers. It sees the api exactly as another container in a deployment
// would: by the name "api", on the network listeners only, with the admin
// listener on the api container's own loopback and out of reach.
//
// It checks rows 1 to 6 of the smoke (row 7, the healthcheck subcommand, needs
// `docker exec` and is checked by the driver on the host) and prints one line
// per check:
//
//	CHECK <row> PASS|FAIL <what> :: <detail>
//
// The driver (scripts/topologysmoke) parses those lines, so their shape is the
// contract between the two. The probe exits 0 when every check passed.
//
// It is compiled by the driver for the Docker engine's own architecture and run
// read-only from a bind mount; it is never built by `make build` or shipped.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

type probe struct {
	publicURL, controlURL string
	adminAddr, publicAddr string
	apiKey, workerName    string
	client                *http.Client
	failed                int
}

func main() {
	p := &probe{
		publicURL:  mustEnv("TOPOLOGY_PUBLIC_URL"),
		controlURL: mustEnv("TOPOLOGY_CONTROL_URL"),
		adminAddr:  mustEnv("TOPOLOGY_ADMIN_ADDR"),
		publicAddr: mustEnv("TOPOLOGY_PUBLIC_ADDR"),
		apiKey:     mustEnv("TOPOLOGY_API_KEY"),
		workerName: mustEnv("TOPOLOGY_WORKER_NAME"),
		client:     &http.Client{Timeout: 10 * time.Second},
	}
	p.rowWorkerRunsAJob()
	p.rowKeyAdministrationIsAbsent()
	p.rowDashboardAndMetricsAreAbsent()
	p.rowControlRefusesForeignAndBrowserRequests()
	p.rowPublicServesNoInternalRoute()
	p.rowAdminIsUnreachable()
	if p.failed > 0 {
		os.Exit(1)
	}
}

func mustEnv(name string) string {
	v := os.Getenv(name)
	if v == "" {
		fmt.Printf("CHECK 0 FAIL the probe's environment :: %s is not set\n", name)
		os.Exit(2)
	}
	return v
}

func (p *probe) check(row int, what string, problem string, okDetail string) {
	if problem != "" {
		p.failed++
		fmt.Printf("CHECK %d FAIL %s :: %s\n", row, what, oneLine(problem))
		return
	}
	fmt.Printf("CHECK %d PASS %s :: %s\n", row, what, oneLine(okDetail))
}

func oneLine(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 300 {
		return s[:300] + "..."
	}
	return s
}

type response struct {
	status int
	code   string
	body   []byte
}

// request sends one request. host, when set, replaces the Host the URL implies;
// headers are added as given.
func (p *probe) request(method, url, host, body string, headers map[string]string) (response, error) {
	var reader io.Reader = http.NoBody
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return response{}, err
	}
	if host != "" {
		req.Host = host
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return response{}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return response{}, err
	}
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(raw, &envelope)
	return response{status: resp.StatusCode, code: envelope.Error.Code, body: raw}, nil
}

func (p *probe) withKey() map[string]string {
	return map[string]string{"Authorization": "Bearer " + p.apiKey}
}

// expect checks one request's status and, when wantCode is set, its error code.
func (p *probe) expect(row int, what, method, url, host string, headers map[string]string, wantStatus int, wantCode string) {
	resp, err := p.request(method, url, host, "", headers)
	switch {
	case err != nil:
		p.check(row, what, err.Error(), "")
	case resp.status != wantStatus || (wantCode != "" && resp.code != wantCode):
		p.check(row, what, fmt.Sprintf("%s %s answered %d %q, want %d %q", method, url, resp.status, resp.code, wantStatus, wantCode), "")
	default:
		p.check(row, what, "", fmt.Sprintf("%s %s -> %d %s", method, url, resp.status, resp.code))
	}
}

// --- row 1 ------------------------------------------------------------------------

// rowWorkerRunsAJob: the worker, in its own container, registered with the
// api's control listener, and a job submitted through the public listener with
// an API key reaches SUCCEEDED.
func (p *probe) rowWorkerRunsAJob() {
	const row = 1
	deadline := time.Now().Add(90 * time.Second)
	var registered string
	for time.Now().Before(deadline) {
		resp, err := p.request(http.MethodGet, p.publicURL+"/v1/workers", "", "", p.withKey())
		if err == nil && resp.status == http.StatusOK {
			var page struct {
				Workers []struct{ ID, Name, Status string } `json:"workers"`
			}
			if json.Unmarshal(resp.body, &page) == nil {
				for _, w := range page.Workers {
					if w.Name == p.workerName && w.Status == "HEALTHY" {
						registered = w.ID
					}
				}
			}
		}
		if registered != "" {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if registered == "" {
		p.check(row, "the worker registered through control:8091", "no HEALTHY worker named "+p.workerName+" in GET /v1/workers within 90s", "")
		return
	}
	p.check(row, "the worker registered through control:8091", "", "GET public:8090/v1/workers lists "+p.workerName+" HEALTHY, id "+registered)

	submit := `{"queue":"default","job_type":"demo.echo","payload":{"message":"topology smoke"}}`
	headers := p.withKey()
	headers["Idempotency-Key"] = fmt.Sprintf("topology-smoke-%d", time.Now().UnixNano())
	resp, err := p.request(http.MethodPost, p.publicURL+"/v1/jobs", "", submit, headers)
	if err != nil || resp.status != http.StatusCreated {
		p.check(row, "a job submitted through public:8090 reaches SUCCEEDED", fmt.Sprintf("POST /v1/jobs: %v status %d %s", err, resp.status, resp.body), "")
		return
	}
	var job struct{ ID, Status string }
	_ = json.Unmarshal(resp.body, &job)

	status := job.Status
	for deadline = time.Now().Add(90 * time.Second); time.Now().Before(deadline) && status != "SUCCEEDED"; {
		time.Sleep(250 * time.Millisecond)
		got, err := p.request(http.MethodGet, p.publicURL+"/v1/jobs/"+job.ID, "", "", p.withKey())
		if err == nil && got.status == http.StatusOK {
			_ = json.Unmarshal(got.body, &job)
			status = job.Status
		}
	}
	if status != "SUCCEEDED" {
		p.check(row, "a job submitted through public:8090 reaches SUCCEEDED", "job "+job.ID+" is "+status+" after 90s", "")
		return
	}
	attempts, err := p.request(http.MethodGet, p.publicURL+"/v1/jobs/"+job.ID+"/attempts", "", "", p.withKey())
	var timeline struct {
		Attempts []struct {
			Status     string `json:"status"`
			WorkerName string `json:"worker_name"`
		} `json:"attempts"`
	}
	if err == nil {
		_ = json.Unmarshal(attempts.body, &timeline)
	}
	if len(timeline.Attempts) != 1 || timeline.Attempts[0].Status != "SUCCEEDED" || timeline.Attempts[0].WorkerName != p.workerName {
		p.check(row, "a job submitted through public:8090 reaches SUCCEEDED", fmt.Sprintf("attempts %s", attempts.body), "")
		return
	}
	p.check(row, "a job submitted through public:8090 reaches SUCCEEDED", "",
		"job "+job.ID+" SUCCEEDED in one attempt by "+p.workerName)
}

// --- rows 2 and 3 --------------------------------------------------------------

// The key-administration routes, with their parameters filled.
var keyRoutes = []struct{ method, path string }{
	{http.MethodGet, "/internal/v1/api-keys"},
	{http.MethodPost, "/internal/v1/api-keys"},
	{http.MethodPost, "/internal/v1/api-keys/11111111-1111-4111-8111-111111111111/revoke"},
	{http.MethodGet, "/internal/v1/worker-keys"},
	{http.MethodPost, "/internal/v1/worker-keys"},
	{http.MethodPost, "/internal/v1/worker-keys/11111111-1111-4111-8111-111111111111/revoke"},
}

func (p *probe) rowKeyAdministrationIsAbsent() {
	const row = 2
	for _, base := range []string{p.publicURL, p.controlURL} {
		for _, rt := range keyRoutes {
			p.expect(row, "key administration is not served", rt.method, base+rt.path, "", nil, http.StatusNotFound, "not_found")
		}
	}
}

func (p *probe) rowDashboardAndMetricsAreAbsent() {
	const row = 3
	for _, base := range []string{p.publicURL, p.controlURL} {
		for _, path := range []string{"/dashboard/", "/dashboard/index.html", "/metrics", "/"} {
			p.expect(row, "the dashboard and /metrics are not served", http.MethodGet, base+path, "", nil, http.StatusNotFound, "not_found")
		}
	}
}

// --- row 4 ------------------------------------------------------------------------

func (p *probe) rowControlRefusesForeignAndBrowserRequests() {
	const row = 4
	claims := p.controlURL + "/internal/v1/claims"
	// The positive control: the allowlisted Host with no browser marking gets
	// past the guard (to request validation), so the refusals below are the
	// guard's and not something else's.
	resp, err := p.request(http.MethodPost, claims, "", "{}", nil)
	switch {
	case err != nil:
		p.check(row, "control:8091 admits the allowlisted Host", err.Error(), "")
	case resp.status == http.StatusForbidden || resp.status == http.StatusNotFound:
		p.check(row, "control:8091 admits the allowlisted Host", fmt.Sprintf("POST /internal/v1/claims with Host api answered %d %q", resp.status, resp.code), "")
	default:
		p.check(row, "control:8091 admits the allowlisted Host", "", fmt.Sprintf("Host api -> %d %s (past the guard)", resp.status, resp.code))
	}
	for _, tc := range []struct {
		what    string
		host    string
		headers map[string]string
	}{
		{"a Host off the allowlist", "evil.example:8091", nil},
		{"a loopback Host, which is not on the allowlist", "127.0.0.1:8091", nil},
		{"a near-miss Host", "api.evil:8091", nil},
		{"the allowlisted Host with Origin", "", map[string]string{"Origin": "http://api:8091"}},
		{"the allowlisted Host with Sec-Fetch-Site", "", map[string]string{"Sec-Fetch-Site": "same-origin"}},
	} {
		resp, err := p.request(http.MethodPost, claims, tc.host, "{}", tc.headers)
		what := "control:8091 refuses " + tc.what
		switch {
		case err != nil:
			p.check(row, what, err.Error(), "")
		case resp.status != http.StatusForbidden || resp.code != "origin_refused":
			p.check(row, what, fmt.Sprintf("answered %d %q, want 403 origin_refused", resp.status, resp.code), "")
		default:
			p.check(row, what, "", "403 origin_refused")
		}
	}
}

// --- row 5 ------------------------------------------------------------------------

// Every /internal route the api has, parameters filled.
var internalRoutes = append([]struct{ method, path string }{
	{http.MethodPut, "/internal/v1/worker-sessions/11111111-1111-4111-8111-111111111111"},
	{http.MethodPost, "/internal/v1/worker-sessions/11111111-1111-4111-8111-111111111111/heartbeat"},
	{http.MethodPost, "/internal/v1/claims"},
	{http.MethodPost, "/internal/v1/leases/11111111-1111-4111-8111-111111111111/renew"},
	{http.MethodPost, "/internal/v1/attempts/11111111-1111-4111-8111-111111111111/start"},
	{http.MethodPost, "/internal/v1/attempts/11111111-1111-4111-8111-111111111111/succeed"},
	{http.MethodPost, "/internal/v1/attempts/11111111-1111-4111-8111-111111111111/fail"},
	{http.MethodPost, "/internal/v1/attempts/11111111-1111-4111-8111-111111111111/cancel"},
}, keyRoutes...)

func (p *probe) rowPublicServesNoInternalRoute() {
	const row = 5
	p.expect(row, "public:8090 serves /v1 (the positive control)", http.MethodGet, p.publicURL+"/v1/jobs", "", p.withKey(), http.StatusOK, "")
	for _, rt := range internalRoutes {
		p.expect(row, "public:8090 serves no /internal route", rt.method, p.publicURL+rt.path, "", nil, http.StatusNotFound, "not_found")
	}
	// And the converse, which the same table decides: control serves no /v1.
	p.expect(row, "control:8091 serves no /v1 route", http.MethodGet, p.controlURL+"/v1/jobs", "", p.withKey(), http.StatusNotFound, "not_found")
}

// --- row 6 ------------------------------------------------------------------------

func (p *probe) rowAdminIsUnreachable() {
	const row = 6
	dial := func(addr string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		if err == nil {
			_ = conn.Close()
		}
		return err
	}
	if err := dial(p.publicAddr); err != nil {
		p.check(row, "the probe can reach the api container (the positive control)", err.Error(), "")
		return
	}
	p.check(row, "the probe can reach the api container (the positive control)", "", "TCP connect to "+p.publicAddr+" succeeded")
	if err := dial(p.adminAddr); err == nil {
		p.check(row, "the admin listener cannot be reached from another container", "TCP connect to "+p.adminAddr+" succeeded", "")
	} else {
		p.check(row, "the admin listener cannot be reached from another container", "", "TCP connect to "+p.adminAddr+" failed: "+err.Error())
	}
	// A request that did get there would be a key listing; make sure it is not.
	if resp, err := p.request(http.MethodGet, "http://"+p.adminAddr+"/internal/v1/api-keys", "", "", nil); err == nil {
		p.check(row, "no key listing is served on the admin port", fmt.Sprintf("answered %d: %s", resp.status, bytes.TrimSpace(resp.body)), "")
	} else {
		p.check(row, "no key listing is served on the admin port", "", "GET failed: "+err.Error())
	}
}
