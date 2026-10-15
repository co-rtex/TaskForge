package objectstore

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/internal/config"
)

// These tests prove TASKFORGE_AWS_DEFAULT_CHAIN reaches the S3 client itself:
// the client built from the loaded configuration through OptionsFromConfig
// carries no endpoint override, no path-style addressing and no static
// credentials. New makes no network call, so neither does this test; nothing
// here retrieves a credential, resolves a host or calls CreateBucket.

func hermetic(t *testing.T) {
	t.Helper()
	t.Setenv("AWS_CONFIG_FILE", os.DevNull)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", os.DevNull)
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3"} {
		unset(t, name)
	}
}

func unset(t *testing.T, name string) {
	t.Helper()
	t.Setenv(name, "")
	require.NoError(t, os.Unsetenv(name))
}

func clientFromEnvironment(t *testing.T) s3.Options {
	t.Helper()
	cfg, err := config.Load()
	require.NoError(t, err)
	c, err := New(context.Background(), OptionsFromConfig(cfg))
	require.NoError(t, err)
	return c.client.Options()
}

func TestClient_AWSDefaultChainReachesTheClient(t *testing.T) {
	hermetic(t)
	for _, name := range []string{
		"TASKFORGE_BROKER_ENDPOINT", "TASKFORGE_BROKER_ACCESS_KEY_ID", "TASKFORGE_BROKER_SECRET_ACCESS_KEY",
		"TASKFORGE_RESULTS_ENDPOINT", "TASKFORGE_RESULTS_ACCESS_KEY_ID", "TASKFORGE_RESULTS_SECRET_ACCESS_KEY",
	} {
		unset(t, name)
	}
	t.Setenv("TASKFORGE_AWS_DEFAULT_CHAIN", "true")
	t.Setenv("TASKFORGE_BROKER_REGION", "eu-west-1")
	t.Setenv("TASKFORGE_RESULTS_REGION", "eu-west-1")

	o := clientFromEnvironment(t)
	require.Nilf(t, o.BaseEndpoint, "no endpoint override: the SDK resolves it from the region; got %q", aws.ToString(o.BaseEndpoint))
	require.False(t, o.UsePathStyle, "real S3 keeps virtual-hosted addressing")
	require.Equal(t, "eu-west-1", o.Region)
	require.NotNil(t, o.Credentials, "the SDK's default chain")
	require.False(t, aws.IsCredentialsProvider(o.Credentials, credentials.StaticCredentialsProvider{}),
		"no static credentials provider, so no \"local\" keys")

	endpoint, err := o.EndpointResolverV2.ResolveEndpoint(context.Background(), s3.EndpointParameters{
		Region: aws.String(o.Region), Bucket: aws.String("taskforge-results"),
	})
	require.NoError(t, err)
	require.Equal(t, "taskforge-results.s3.eu-west-1.amazonaws.com", endpoint.URI.Host)
	require.Equal(t, "https", endpoint.URI.Scheme)
}

func TestClient_LocalDefaultsReachTheClient(t *testing.T) {
	hermetic(t)
	for _, name := range []string{"TASKFORGE_AWS_DEFAULT_CHAIN", "TASKFORGE_RESULTS_ENDPOINT", "TASKFORGE_RESULTS_ACCESS_KEY_ID", "TASKFORGE_RESULTS_SECRET_ACCESS_KEY", "TASKFORGE_RESULTS_REGION"} {
		unset(t, name)
	}
	o := clientFromEnvironment(t)
	require.Equal(t, "http://127.0.0.1:4566", aws.ToString(o.BaseEndpoint))
	require.True(t, o.UsePathStyle)
	require.True(t, aws.IsCredentialsProvider(o.Credentials, credentials.StaticCredentialsProvider{}))
}
