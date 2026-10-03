package stack

import (
	"fmt"

	"github.com/co-rtex/TaskForge/internal/config"
)

// dialed lists, for each binary, the environment variables that carry the
// address of something it connects to. It was read off cmd/<binary>/main.go:
// which of the database URL, the broker endpoint, the object store endpoint and
// the worker's API URL that binary actually uses. A binary started without one
// of them would silently take its loopback default, which is wrong the moment
// the infrastructure is not on this machine's own loopback.
var dialed = map[string][]string{
	"taskforge-api":        {"TASKFORGE_DATABASE_URL", "TASKFORGE_RESULTS_ENDPOINT"},
	"taskforge-outbox":     {"TASKFORGE_DATABASE_URL", "TASKFORGE_BROKER_ENDPOINT"},
	"taskforge-reconciler": {"TASKFORGE_DATABASE_URL"},
	"taskforge-scheduler":  {"TASKFORGE_DATABASE_URL"},
	"taskforge-worker": {
		"TASKFORGE_BROKER_ENDPOINT", "TASKFORGE_RESULTS_ENDPOINT", "TASKFORGE_WORKER_API_URL",
	},
}

// Infra is where this run's infrastructure is, read from the same variables the
// services read.
type Infra struct {
	DatabaseURL string

	BrokerEndpoint, BrokerRegion, BrokerAccessKeyID, BrokerSecretAccessKey string

	ResultsEndpoint, ResultsBucket, ResultsRegion,
	ResultsAccessKeyID, ResultsSecretAccessKey string
}

// LoadInfra resolves the infrastructure with the services' own loader, so the
// variable names, the .env handling and the compose-default fallbacks are not
// restated here. `make demo` depends on `make migrate`, which reads the same
// .env, so both land on the same database.
func LoadInfra() (Infra, error) {
	if err := config.LoadDotEnv(".env"); err != nil {
		return Infra{}, fmt.Errorf("read .env: %w", err)
	}
	cfg, err := config.Load()
	if err != nil {
		return Infra{}, fmt.Errorf("the TASKFORGE_* configuration is invalid: %w", err)
	}
	return Infra{
		DatabaseURL:    cfg.DatabaseURL,
		BrokerEndpoint: cfg.BrokerEndpoint, BrokerRegion: cfg.BrokerRegion,
		BrokerAccessKeyID: cfg.BrokerAccessKeyID, BrokerSecretAccessKey: cfg.BrokerSecretAccessKey,
		ResultsEndpoint: cfg.ResultsEndpoint, ResultsBucket: cfg.ResultsBucket,
		ResultsRegion: cfg.ResultsRegion, ResultsAccessKeyID: cfg.ResultsAccessKeyID,
		ResultsSecretAccessKey: cfg.ResultsSecretAccessKey,
	}, nil
}
