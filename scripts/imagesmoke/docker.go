package main

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

// docker runs the docker CLI and returns what it printed. A non-zero exit is not
// an error here: the exit code is the observation (a container refusing its
// configuration is supposed to exit non-zero). The error is only for being unable
// to run docker at all.
type dockerOutput struct {
	stdout string
	stderr string
	exit   int
}

func (d dockerOutput) combined() string { return d.stdout + d.stderr }

func docker(ctx context.Context, args ...string) (dockerOutput, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	out := dockerOutput{stdout: stdout.String(), stderr: stderr.String()}
	var exitErr *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exitErr):
		out.exit = exitErr.ExitCode()
	default:
		return out, fmt.Errorf("run docker %s: %w", strings.Join(args[:min(len(args), 2)], " "), err)
	}
	return out, nil
}

// imageInfo is the part of `docker image inspect` the smoke judges.
type imageInfo struct {
	ID           string `json:"Id"`
	Architecture string `json:"Architecture"`
	OS           string `json:"Os"`
	Size         int64  `json:"Size"`
	Config       struct {
		User       string            `json:"User"`
		Entrypoint []string          `json:"Entrypoint"`
		Labels     map[string]string `json:"Labels"`
	} `json:"Config"`
}

func inspectImage(ctx context.Context, image string) (imageInfo, error) {
	out, err := docker(ctx, "image", "inspect", image)
	if err != nil {
		return imageInfo{}, err
	}
	if out.exit != 0 {
		return imageInfo{}, fmt.Errorf("%s is not built (docker image inspect exit %d); run `make images` first:\n%s",
			image, out.exit, truncate(out.stderr, 300))
	}
	var infos []imageInfo
	if err := json.Unmarshal([]byte(out.stdout), &infos); err != nil || len(infos) != 1 {
		return imageInfo{}, fmt.Errorf("unreadable docker image inspect output for %s", image)
	}
	return infos[0], nil
}

// imageFiles lists every path in an image's filesystem by creating a container
// from it, exporting that, and reading the tar stream. It never starts the
// container, and it removes it. (`docker rm` has no -q.)
func imageFiles(ctx context.Context, image string) ([]string, error) {
	created, err := docker(ctx, "create", image)
	if err != nil {
		return nil, err
	}
	if created.exit != 0 {
		return nil, fmt.Errorf("docker create %s exit %d: %s", image, created.exit, truncate(created.stderr, 300))
	}
	id := strings.TrimSpace(created.stdout)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_, _ = docker(cleanup, "rm", "--force", id)
	}()

	exported, err := docker(ctx, "export", id)
	if err != nil {
		return nil, err
	}
	if exported.exit != 0 {
		return nil, fmt.Errorf("docker export exit %d: %s", exported.exit, truncate(exported.stderr, 300))
	}
	var files []string
	reader := tar.NewReader(strings.NewReader(exported.stdout))
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return files, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read the exported filesystem: %w", err)
		}
		files = append(files, header.Name)
	}
}
