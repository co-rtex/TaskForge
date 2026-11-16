package queue

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNotificationQueueName_IsStablePerAddressAndDistinctAcrossAddresses(t *testing.T) {
	a := NotificationQueueName("taskforge-work-available", Address{Scope: "tenant-a", Queue: "default"})
	require.Equal(t, a, NotificationQueueName("taskforge-work-available", Address{Scope: "tenant-a", Queue: "default"}),
		"the publisher and a worker must derive the same name independently")
	require.True(t, strings.HasPrefix(a, "taskforge-work-available-"))
	require.Len(t, a, len("taskforge-work-available-")+addressDigestLength)

	seen := map[string]Address{}
	for _, address := range []Address{
		{Scope: "tenant-a", Queue: "default"},
		{Scope: "tenant-b", Queue: "default"},
		{Scope: "tenant-a", Queue: "batch"},
		// The separator must keep these two apart.
		{Scope: "a", Queue: "b-c"},
		{Scope: "a-b", Queue: "c"},
	} {
		name := NotificationQueueName("base", address)
		previous, clash := seen[name]
		require.Falsef(t, clash, "%+v and %+v share a broker queue", address, previous)
		seen[name] = address
	}
}

func TestNotificationQueueName_FitsSQSForTheLongestValidBase(t *testing.T) {
	base := strings.Repeat("q", MaxBaseQueueNameLength)
	require.NoError(t, ValidateBaseQueueName(base))
	name := NotificationQueueName(base, Address{Scope: strings.Repeat("s", 128), Queue: "default"})
	require.Len(t, name, 80)
	require.Regexp(t, `^[A-Za-z0-9_-]{1,80}$`, name)
}

func TestValidateBaseQueueName(t *testing.T) {
	require.NoError(t, ValidateBaseQueueName("taskforge-work-available"))
	require.Error(t, ValidateBaseQueueName(""))
	require.Error(t, ValidateBaseQueueName("work.fifo"), "a FIFO name is not a standard queue")
	require.Error(t, ValidateBaseQueueName("has space"))
	require.Error(t, ValidateBaseQueueName(strings.Repeat("q", MaxBaseQueueNameLength+1)))
}
