package worker

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRegistry_RejectsDuplicateHandlersAndSortsTypes(t *testing.T) {
	registry := NewRegistry()
	require.NoError(t, registry.Register("demo.z", DemoEcho{}))
	require.NoError(t, registry.Register("demo.a", DemoEcho{}))
	require.Error(t, registry.Register("demo.a", DemoEcho{}))
	require.Equal(t, []string{"demo.a", "demo.z"}, registry.Types())
}

func TestDemoEcho_ReturnsAnIndependentExactCopy(t *testing.T) {
	payload := json.RawMessage(`{"message":"hello","n":9007199254740993}`)
	result, err := (DemoEcho{}).Execute(context.Background(), Execution{
		JobID: uuid.New(), AttemptID: uuid.New(), Payload: payload,
	})
	require.NoError(t, err)
	require.Equal(t, payload, result)
	result[0] = '['
	require.Equal(t, byte('{'), payload[0], "handler result must not alias the authoritative payload")
}

func TestInvokeHandler_ContainsAPanic(t *testing.T) {
	_, err := invokeHandler(context.Background(), HandlerFunc(func(context.Context, Execution) (json.RawMessage, error) {
		panic("boom")
	}), Execution{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "handler panic")
}
