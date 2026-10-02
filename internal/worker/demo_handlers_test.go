package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	yaml "go.yaml.in/yaml/v3"

	"github.com/co-rtex/TaskForge/internal/jobs"
	"github.com/co-rtex/TaskForge/internal/lifecycle"
)

// These tests cover the two failure-demonstration handlers, demo.sleep and
// demo.fail. The only real time in them is demo.sleep's own timer, because
// waiting is the whole of what that handler does: the durations are tens of
// milliseconds, and every assertion about cancellation is measured from the
// cancel rather than from the requested duration, so none of them depends on
// how fast the machine is.

// maxSleepMillis is the largest duration demo.sleep accepts, derived from the
// same constant that bounds a job's own timeout_seconds rather than copied.
const maxSleepMillis = int64(jobs.MaxTimeoutSeconds) * 1000

func execute(ctx context.Context, handler Handler, payload string) (json.RawMessage, error) {
	return handler.Execute(ctx, Execution{
		JobID: uuid.New(), AttemptID: uuid.New(), Payload: json.RawMessage(payload),
	})
}

// requireFailure asserts err is a handler-declared failure of the given class
// and code, and returns it.
func requireFailure(t *testing.T, err error, class lifecycle.FailureClass, code string) *FailureError {
	t.Helper()
	var failure *FailureError
	require.ErrorAs(t, err, &failure, "a rejected payload must be a declared failure, not a plain error")
	require.Equal(t, class, failure.Class)
	require.Equal(t, code, failure.Code)
	return failure
}

func TestDemoSleep_ReturnsTheResultAfterRoughlyTheRequestedDuration(t *testing.T) {
	const requested = 60

	started := time.Now()
	result, err := execute(context.Background(), DemoSleep{}, `{"duration_ms":60}`)
	elapsed := time.Since(started)

	require.NoError(t, err)
	require.GreaterOrEqual(t, elapsed, requested*time.Millisecond, "it must actually wait")
	require.Less(t, elapsed, requested*time.Millisecond+5*time.Second, "and not wait much longer than asked")
	require.JSONEq(t, `{"slept_ms":60}`, string(result))
}

func TestDemoSleep_ResultBodyReportsTheRequestedDuration(t *testing.T) {
	result, err := execute(context.Background(), DemoSleep{}, `{"duration_ms":1}`)
	require.NoError(t, err)
	require.JSONEq(t, `{"slept_ms":1}`, string(result))
	require.True(t, json.Valid(result))
}

func TestDemoSleep_ReturnsPromptlyAfterCancellationNotAfterTheRequestedDuration(t *testing.T) {
	// One hour. If the handler waited for its timer instead of the context, this
	// test would not finish.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type outcome struct {
		result     json.RawMessage
		err        error
		returnedAt time.Time
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := execute(ctx, DemoSleep{}, `{"duration_ms":3600000}`)
		done <- outcome{result, err, time.Now()}
	}()

	// It must be genuinely waiting, not have returned already.
	select {
	case early := <-done:
		t.Fatalf("returned before it was canceled: %+v", early)
	case <-time.After(50 * time.Millisecond):
	}

	canceledAt := time.Now()
	cancel()
	select {
	case got := <-done:
		require.ErrorIs(t, got.err, context.Canceled)
		require.Nil(t, got.result, "a canceled sleep has no result to report")
		require.Less(t, got.returnedAt.Sub(canceledAt), time.Second,
			"return must be measured from the cancel, not from the one-hour duration")
	case <-time.After(10 * time.Second):
		t.Fatal("demo.sleep ignored its context and kept waiting")
	}
}

func TestDemoSleep_ReturnsPromptlyWhenTheDeadlineEndsTheContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	started := time.Now()
	result, err := execute(ctx, DemoSleep{}, `{"duration_ms":3600000}`)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, result)
	require.Less(t, time.Since(started), 5*time.Second)
}

func TestDemoSleep_AContextThatIsAlreadyOverReturnsWithoutWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := execute(ctx, DemoSleep{}, `{"duration_ms":3600000}`)

	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result)
}

func TestDemoSleep_AcceptsBothInclusiveBounds(t *testing.T) {
	// Run under an already-canceled context so the largest value does not
	// actually sleep for a day. What is being asked is only whether the payload
	// was accepted: an accepted one reaches the wait and returns the context's
	// error, a rejected one would return a declared failure instead.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for name, payload := range map[string]string{
		"smallest": `{"duration_ms":1}`,
		"largest":  fmt.Sprintf(`{"duration_ms":%d}`, maxSleepMillis),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := execute(ctx, DemoSleep{}, payload)
			require.ErrorIs(t, err, context.Canceled)
			var failure *FailureError
			require.False(t, errors.As(err, &failure), "the bound is inclusive: %s must be accepted", payload)
		})
	}
}

func TestDemoSleep_RejectsEveryInvalidPayloadAsPermanentInvalidPayload(t *testing.T) {
	cases := map[string]string{
		"unknown field":             `{"duration_ms":5,"extra":1}`,
		"only an unknown field":     `{"duration":5}`,
		"missing field":             `{}`,
		"null duration":             `{"duration_ms":null}`,
		"zero":                      `{"duration_ms":0}`,
		"negative":                  `{"duration_ms":-1}`,
		"one past the maximum":      fmt.Sprintf(`{"duration_ms":%d}`, maxSleepMillis+1),
		"far past int64":            `{"duration_ms":99999999999999999999}`,
		"fractional":                `{"duration_ms":5.5}`,
		"whole number with a point": `{"duration_ms":5.0}`,
		"exponent form":             `{"duration_ms":1e3}`,
		"string":                    `{"duration_ms":"5"}`,
		"boolean":                   `{"duration_ms":true}`,
		"array":                     `[5]`,
		"bare number":               `5`,
		"bare null":                 `null`,
		"empty":                     ``,
		"trailing second object":    `{"duration_ms":5}{"duration_ms":5}`,
		"trailing garbage":          `{"duration_ms":5} x`,
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			// Bounded so a regression that accepts the payload fails here rather
			// than sleeping for the duration it asked for.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			result, err := execute(ctx, DemoSleep{}, payload)

			requireFailure(t, err, lifecycle.ClassPermanent, "invalid_payload")
			require.Nil(t, result)
		})
	}
}

func TestDemoFail_MapsEachClassToItsFailureClassWithAFixedCode(t *testing.T) {
	for payload, want := range map[string]lifecycle.FailureClass{
		`{"class":"retryable"}`: lifecycle.ClassRetryable,
		`{"class":"permanent"}`: lifecycle.ClassPermanent,
	} {
		t.Run(payload, func(t *testing.T) {
			result, err := execute(context.Background(), DemoFail{}, payload)

			failure := requireFailure(t, err, want, "demo_failure")
			require.Nil(t, result)
			require.NotEmpty(t, failure.Message)
			require.NoError(t, lifecycle.ValidateErrorMessage(failure.Message))

			// What the runner would actually record: the declared class and code
			// survive classification rather than being downgraded to the generic
			// retryable handler_error.
			class, code, message := classifyHandlerError(err)
			require.Equal(t, want, class)
			require.Equal(t, "demo_failure", code)
			require.Equal(t, failure.Message, message)
		})
	}
}

func TestDemoFail_TheTwoClassesAreDistinct(t *testing.T) {
	_, retryable := execute(context.Background(), DemoFail{}, `{"class":"retryable"}`)
	_, permanent := execute(context.Background(), DemoFail{}, `{"class":"permanent"}`)

	var r, p *FailureError
	require.ErrorAs(t, retryable, &r)
	require.ErrorAs(t, permanent, &p)
	require.NotEqual(t, r.Class, p.Class)
	require.NotEqual(t, r.Message, p.Message, "each message says which class was asked for")
}

func TestDemoFail_RejectsEveryInvalidPayloadAsPermanentInvalidPayload(t *testing.T) {
	cases := map[string]string{
		"missing class":          `{}`,
		"unknown class":          `{"class":"transient"}`,
		"wrong case":             `{"class":"RETRYABLE"}`,
		"padded":                 `{"class":" retryable"}`,
		"empty class":            `{"class":""}`,
		"null class":             `{"class":null}`,
		"number":                 `{"class":5}`,
		"unknown extra field":    `{"class":"retryable","code":"anything"}`,
		"only an unknown field":  `{"code":"anything"}`,
		"array":                  `["retryable"]`,
		"bare string":            `"retryable"`,
		"bare null":              `null`,
		"empty":                  ``,
		"trailing second object": `{"class":"retryable"}{"class":"permanent"}`,
		"trailing garbage":       `{"class":"retryable"} x`,
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			result, err := execute(context.Background(), DemoFail{}, payload)

			requireFailure(t, err, lifecycle.ClassPermanent, "invalid_payload")
			require.Nil(t, result)
		})
	}
}

// TestDemoHandlers_NeverEchoThePayloadIntoAFailure is the AGENTS.md section 10
// property that matters most here: a failure's code and message are stored,
// returned, and listed, so a payload that reached them would be a channel from
// a caller's data to every reader of the dead-letter queue.
func TestDemoHandlers_NeverEchoThePayloadIntoAFailure(t *testing.T) {
	secret := "SECRET-" + uuid.NewString()

	for name, run := range map[string]func() error{
		"demo.fail, unknown class value": func() error {
			_, err := execute(context.Background(), DemoFail{}, fmt.Sprintf(`{"class":%q}`, secret))
			return err
		},
		"demo.fail, unknown field name": func() error {
			_, err := execute(context.Background(), DemoFail{}, fmt.Sprintf(`{"class":"retryable",%q:1}`, secret))
			return err
		},
		"demo.fail, a valid class": func() error {
			_, err := execute(context.Background(), DemoFail{}, `{"class":"retryable"}`)
			return err
		},
		"demo.sleep, unknown field name": func() error {
			_, err := execute(context.Background(), DemoSleep{}, fmt.Sprintf(`{"duration_ms":5,%q:1}`, secret))
			return err
		},
		"demo.sleep, string value": func() error {
			_, err := execute(context.Background(), DemoSleep{}, fmt.Sprintf(`{"duration_ms":%q}`, secret))
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := run()
			var failure *FailureError
			require.ErrorAs(t, err, &failure)
			require.NotContains(t, failure.Message, secret)
			require.NotContains(t, failure.Code, secret)
			require.NotContains(t, err.Error(), secret)

			_, code, message := classifyHandlerError(err)
			require.NotContains(t, code, secret)
			require.NotContains(t, message, secret)
		})
	}
}

// TestDemoHandlers_FailureCodesAndMessagesAreValidAndFixed binds both handlers'
// codes to the pattern api/openapi.yaml documents for a worker-reported
// error_code, read from the document itself rather than copied here.
func TestDemoHandlers_FailureCodesAndMessagesAreValidAndFixed(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "api", "openapi.yaml"))
	require.NoError(t, err)
	var spec struct {
		Components struct {
			Schemas map[string]struct {
				Properties map[string]struct {
					Pattern string `yaml:"pattern"`
				} `yaml:"properties"`
			} `yaml:"schemas"`
		} `yaml:"components"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &spec))
	documented := spec.Components.Schemas["FailureReport"].Properties["error_code"].Pattern
	require.NotEmpty(t, documented, "FailureReport.error_code must carry a pattern in api/openapi.yaml")
	pattern := regexp.MustCompile(documented)

	var produced []*FailureError
	for handler, payloads := range map[Handler][]string{
		DemoFail{}:  {`{"class":"retryable"}`, `{"class":"permanent"}`, `{}`},
		DemoSleep{}: {`{}`},
	} {
		for _, payload := range payloads {
			_, err := execute(context.Background(), handler, payload)
			var failure *FailureError
			require.ErrorAs(t, err, &failure)
			produced = append(produced, failure)
		}
	}
	require.Len(t, produced, 4)

	for _, failure := range produced {
		require.Regexpf(t, pattern, failure.Code, "code %q must match the documented pattern %s", failure.Code, documented)
		require.NoError(t, lifecycle.ValidateErrorCode(failure.Code))
		require.NoError(t, lifecycle.ValidateErrorMessage(failure.Message))
		require.False(t, strings.ContainsAny(failure.Message, "\r\n"))
	}
}
