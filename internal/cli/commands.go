package cli

import (
	"context"
	"encoding/json"
	"flag"
	"io"
)

// jobSuccess is shared by every route whose success is "200 (idempotent
// replay) or 201 (created)": job submission and DLQ replay/retry both
// document this pair identically in api/openapi.yaml.
var jobSuccess = map[int]bool{200: true, 201: true}
var readSuccess = map[int]bool{200: true}

func cmdJobsSubmit(ctx context.Context, c *Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("jobs submit", flag.ContinueOnError)
	queue := fs.String("queue", "", "queue name (required)")
	jobType := fs.String("job-type", "", "job type (required)")
	payload := fs.String("payload", "", "JSON object payload (required)")
	priority := fs.Int("priority", 50, "priority, 0-100")
	maxAttempts := fs.Int("max-attempts", 3, "max attempts, including the first")
	timeoutSeconds := fs.Int("timeout-seconds", 300, "per-attempt execution budget in seconds")
	scheduledAt := fs.String("scheduled-at", "", "RFC 3339 earliest execution instant; omit for immediate")
	var capabilities stringSliceFlag
	fs.Var(&capabilities, "capability", "required capability (repeatable)")
	idempotencyKey := fs.String("idempotency-key", "", "idempotency key; a random one is generated if omitted")

	if ok, code := parseFlags(fs, stderr, args); !ok {
		return code
	}
	if *queue == "" || *jobType == "" || *payload == "" {
		return usageErrorf(stderr, "jobs submit requires --queue, --job-type, and --payload")
	}
	var payloadObj map[string]any
	if err := json.Unmarshal([]byte(*payload), &payloadObj); err != nil {
		return usageErrorf(stderr, "--payload must be a JSON object: %v", err)
	}

	req := SubmitJobRequest{
		Queue:                *queue,
		JobType:              *jobType,
		Payload:              payloadObj,
		Priority:             *priority,
		MaxAttempts:          *maxAttempts,
		TimeoutSeconds:       *timeoutSeconds,
		RequiredCapabilities: []string(capabilities),
	}
	if *scheduledAt != "" {
		req.ScheduledAt = scheduledAt
	}
	body, err := json.Marshal(req)
	if err != nil {
		return usageErrorf(stderr, "encode request: %v", err)
	}

	resp, err := c.SubmitJob(ctx, newIdempotencyKeyIfEmpty(*idempotencyKey), body)
	if err != nil {
		return transportResult(err, stderr)
	}
	return emit(resp, jobSuccess, stdout, stderr)
}

func cmdJobsGet(ctx context.Context, c *Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("jobs get", flag.ContinueOnError)
	if ok, code := parseFlags(fs, stderr, args); !ok {
		return code
	}
	if fs.NArg() != 1 {
		return usageErrorf(stderr, "jobs get requires exactly one argument: <job_id>")
	}
	resp, err := c.GetJob(ctx, fs.Arg(0))
	if err != nil {
		return transportResult(err, stderr)
	}
	return emit(resp, readSuccess, stdout, stderr)
}

func cmdJobsResult(ctx context.Context, c *Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("jobs result", flag.ContinueOnError)
	if ok, code := parseFlags(fs, stderr, args); !ok {
		return code
	}
	if fs.NArg() != 1 {
		return usageErrorf(stderr, "jobs result requires exactly one argument: <job_id>")
	}
	resp, err := c.GetResult(ctx, fs.Arg(0))
	if err != nil {
		return transportResult(err, stderr)
	}
	return emit(resp, readSuccess, stdout, stderr)
}

func cmdJobsCancel(ctx context.Context, c *Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("jobs cancel", flag.ContinueOnError)
	if ok, code := parseFlags(fs, stderr, args); !ok {
		return code
	}
	if fs.NArg() != 1 {
		return usageErrorf(stderr, "jobs cancel requires exactly one argument: <job_id>")
	}
	resp, err := c.CancelJob(ctx, fs.Arg(0))
	if err != nil {
		return transportResult(err, stderr)
	}
	return emit(resp, readSuccess, stdout, stderr)
}

func cmdJobsRetry(ctx context.Context, c *Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("jobs retry", flag.ContinueOnError)
	idempotencyKey := fs.String("idempotency-key", "", "idempotency key; a random one is generated if omitted")
	if ok, code := parseFlags(fs, stderr, args); !ok {
		return code
	}
	if fs.NArg() != 1 {
		return usageErrorf(stderr, "jobs retry requires exactly one argument: <job_id>")
	}
	resp, err := c.RetryJob(ctx, fs.Arg(0), newIdempotencyKeyIfEmpty(*idempotencyKey))
	if err != nil {
		return transportResult(err, stderr)
	}
	return emit(resp, jobSuccess, stdout, stderr)
}

// cmdJobsList runs `jobs list`. The page is bounded by the server; this
// command deliberately does NOT follow next_cursor itself. A CLI that
// silently paged an unbounded listing would turn one documented request
// into an arbitrary number of them, and a script that wants every page can
// loop on the cursor it is handed.
func cmdJobsList(ctx context.Context, c *Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("jobs list", flag.ContinueOnError)
	status := fs.String("status", "", "only jobs in this state (server-validated)")
	queue := fs.String("queue", "", "only jobs in this queue")
	limit := fs.Int("limit", 0, "page size, 1-100 (server default applies if omitted)")
	cursor := fs.String("cursor", "", "next_cursor from a previous page")
	if ok, code := parseFlags(fs, stderr, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return usageErrorf(stderr, "jobs list takes no positional arguments")
	}
	resp, err := c.ListJobs(ctx, *status, *queue, *limit, *cursor)
	if err != nil {
		return transportResult(err, stderr)
	}
	return emit(resp, readSuccess, stdout, stderr)
}

// cmdJobsAttempts runs `jobs attempts`. Unpaginated, like the route.
func cmdJobsAttempts(ctx context.Context, c *Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("jobs attempts", flag.ContinueOnError)
	if ok, code := parseFlags(fs, stderr, args); !ok {
		return code
	}
	if fs.NArg() != 1 {
		return usageErrorf(stderr, "jobs attempts requires exactly one argument: <job_id>")
	}
	resp, err := c.ListJobAttempts(ctx, fs.Arg(0))
	if err != nil {
		return transportResult(err, stderr)
	}
	return emit(resp, readSuccess, stdout, stderr)
}

// cmdWorkersList runs `workers list`.
func cmdWorkersList(ctx context.Context, c *Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("workers list", flag.ContinueOnError)
	limit := fs.Int("limit", 0, "page size, 1-100 (server default applies if omitted)")
	cursor := fs.String("cursor", "", "next_cursor from a previous page")
	if ok, code := parseFlags(fs, stderr, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return usageErrorf(stderr, "workers list takes no positional arguments")
	}
	resp, err := c.ListWorkers(ctx, *limit, *cursor)
	if err != nil {
		return transportResult(err, stderr)
	}
	return emit(resp, readSuccess, stdout, stderr)
}

// cmdQueuesList runs `queues list`. Unpaginated, like the route.
func cmdQueuesList(ctx context.Context, c *Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("queues list", flag.ContinueOnError)
	if ok, code := parseFlags(fs, stderr, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return usageErrorf(stderr, "queues list takes no positional arguments")
	}
	resp, err := c.ListQueues(ctx)
	if err != nil {
		return transportResult(err, stderr)
	}
	return emit(resp, readSuccess, stdout, stderr)
}

func cmdDLQList(ctx context.Context, c *Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("dlq list", flag.ContinueOnError)
	limit := fs.Int("limit", 0, "page size, 1-100 (server default applies if omitted)")
	cursor := fs.String("cursor", "", "next_cursor from a previous page")
	if ok, code := parseFlags(fs, stderr, args); !ok {
		return code
	}
	resp, err := c.ListDLQ(ctx, *limit, *cursor)
	if err != nil {
		return transportResult(err, stderr)
	}
	return emit(resp, readSuccess, stdout, stderr)
}

func cmdDLQReplay(ctx context.Context, c *Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("dlq replay", flag.ContinueOnError)
	idempotencyKey := fs.String("idempotency-key", "", "idempotency key; a random one is generated if omitted")
	if ok, code := parseFlags(fs, stderr, args); !ok {
		return code
	}
	if fs.NArg() != 1 {
		return usageErrorf(stderr, "dlq replay requires exactly one argument: <job_id>")
	}
	resp, err := c.ReplayDLQ(ctx, fs.Arg(0), newIdempotencyKeyIfEmpty(*idempotencyKey))
	if err != nil {
		return transportResult(err, stderr)
	}
	return emit(resp, jobSuccess, stdout, stderr)
}

func cmdAPIKeysCreate(ctx context.Context, c *Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("api-keys create", flag.ContinueOnError)
	scope := fs.String("scope", "", "tenancy scope this key acts within (required)")
	name := fs.String("name", "", "operator-facing label (required)")
	if ok, code := parseFlags(fs, stderr, args); !ok {
		return code
	}
	if *scope == "" || *name == "" {
		return usageErrorf(stderr, "api-keys create requires --scope and --name")
	}
	body, err := json.Marshal(apiKeyCreateRequest{Scope: *scope, Name: *name})
	if err != nil {
		return usageErrorf(stderr, "encode request: %v", err)
	}
	resp, err := c.CreateAPIKey(ctx, body)
	if err != nil {
		return transportResult(err, stderr)
	}
	return emit(resp, map[int]bool{201: true}, stdout, stderr)
}

func cmdAPIKeysList(ctx context.Context, c *Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("api-keys list", flag.ContinueOnError)
	limit := fs.Int("limit", 0, "page size, 1-200 (server default applies if omitted)")
	if ok, code := parseFlags(fs, stderr, args); !ok {
		return code
	}
	resp, err := c.ListAPIKeys(ctx, *limit)
	if err != nil {
		return transportResult(err, stderr)
	}
	return emit(resp, readSuccess, stdout, stderr)
}

func cmdAPIKeysRevoke(ctx context.Context, c *Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("api-keys revoke", flag.ContinueOnError)
	if ok, code := parseFlags(fs, stderr, args); !ok {
		return code
	}
	if fs.NArg() != 1 {
		return usageErrorf(stderr, "api-keys revoke requires exactly one argument: <key_id>")
	}
	resp, err := c.RevokeAPIKey(ctx, fs.Arg(0))
	if err != nil {
		return transportResult(err, stderr)
	}
	return emit(resp, readSuccess, stdout, stderr)
}

func cmdWorkerKeysCreate(ctx context.Context, c *Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("worker-keys create", flag.ContinueOnError)
	scope := fs.String("scope", "", "scope a session registering with this key acts within (required)")
	name := fs.String("name", "", "operator-facing label (required)")
	if ok, code := parseFlags(fs, stderr, args); !ok {
		return code
	}
	if *scope == "" || *name == "" {
		return usageErrorf(stderr, "worker-keys create requires --scope and --name")
	}
	body, err := json.Marshal(apiKeyCreateRequest{Scope: *scope, Name: *name})
	if err != nil {
		return usageErrorf(stderr, "encode request: %v", err)
	}
	resp, err := c.CreateWorkerKey(ctx, body)
	if err != nil {
		return transportResult(err, stderr)
	}
	return emit(resp, map[int]bool{201: true}, stdout, stderr)
}

func cmdWorkerKeysList(ctx context.Context, c *Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("worker-keys list", flag.ContinueOnError)
	limit := fs.Int("limit", 0, "page size, 1-200 (server default applies if omitted)")
	if ok, code := parseFlags(fs, stderr, args); !ok {
		return code
	}
	resp, err := c.ListWorkerKeys(ctx, *limit)
	if err != nil {
		return transportResult(err, stderr)
	}
	return emit(resp, readSuccess, stdout, stderr)
}

func cmdWorkerKeysRevoke(ctx context.Context, c *Client, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("worker-keys revoke", flag.ContinueOnError)
	if ok, code := parseFlags(fs, stderr, args); !ok {
		return code
	}
	if fs.NArg() != 1 {
		return usageErrorf(stderr, "worker-keys revoke requires exactly one argument: <key_id>")
	}
	resp, err := c.RevokeWorkerKey(ctx, fs.Arg(0))
	if err != nil {
		return transportResult(err, stderr)
	}
	return emit(resp, readSuccess, stdout, stderr)
}
