package main

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

// Environment is the machine and toolchain a record was measured on. A number
// without it is not reproducible and not comparable.
type Environment struct {
	CPUModel      string `json:"cpu_model"`
	LogicalCores  int    `json:"logical_cores"`
	PhysicalCores int    `json:"physical_cores,omitempty"`
	MemoryBytes   uint64 `json:"memory_bytes"`
	OS            string `json:"os"`
	// PowerSource matters: a laptop on battery is throttled and sleeps.
	PowerSource string `json:"power_source"`
	// LowPowerMode matters as much: macOS caps CPU performance while it is on.
	LowPowerMode string `json:"low_power_mode"`

	GoVersion       string   `json:"go_version"`
	DockerClient    string   `json:"docker_client_version"`
	DockerServer    string   `json:"docker_server_version"`
	DockerVMCPUs    int      `json:"docker_vm_cpus,omitempty"`
	DockerVMMemory  int64    `json:"docker_vm_memory_bytes,omitempty"`
	PostgresVersion string   `json:"postgresql_server_version"`
	ComposeImages   []string `json:"compose_images"`
}

// commandFunc runs a command and returns its standard output.
type commandFunc func(name string, args ...string) (string, error)

func runCommand(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).Output()
	return string(out), err
}

// captureEnvironment reads the environment. A tool that is missing or fails
// leaves its field saying so; it never stops the run being recorded and its
// field is never invented.
func captureEnvironment(run commandFunc, goos, postgresVersion string) Environment {
	env := Environment{
		GoVersion:       runtime.Version() + " " + runtime.GOOS + "/" + runtime.GOARCH,
		PostgresVersion: postgresVersion,
		LogicalCores:    runtime.NumCPU(),
		CPUModel:        "unknown", OS: "unknown", PowerSource: "unknown", LowPowerMode: "unknown",
	}

	switch goos {
	case "darwin":
		env.CPUModel = firstLine(sysctl(run, "machdep.cpu.brand_string"), "unknown")
		if n, err := strconv.Atoi(firstLine(sysctl(run, "hw.logicalcpu"), "")); err == nil {
			env.LogicalCores = n
		}
		if n, err := strconv.Atoi(firstLine(sysctl(run, "hw.physicalcpu"), "")); err == nil {
			env.PhysicalCores = n
		}
		if n, err := strconv.ParseUint(firstLine(sysctl(run, "hw.memsize"), ""), 10, 64); err == nil {
			env.MemoryBytes = n
		}
		if v, err := run("sw_vers", "-productVersion"); err == nil {
			env.OS = "macOS " + strings.TrimSpace(v)
			if arch, err := run("uname", "-m"); err == nil {
				env.OS += " (" + strings.TrimSpace(arch) + ")"
			}
		}
		if out, err := run("pmset", "-g", "batt"); err == nil {
			env.PowerSource = parsePower(out)
		}
		if out, err := run("pmset", "-g"); err == nil {
			env.LowPowerMode = parseLowPowerMode(out)
		}
	case "linux":
		if raw, err := os.ReadFile("/proc/cpuinfo"); err == nil {
			if m := parseCPUInfoModel(string(raw)); m != "" {
				env.CPUModel = m
			}
		}
		if raw, err := os.ReadFile("/proc/meminfo"); err == nil {
			env.MemoryBytes = parseMemInfoBytes(string(raw))
		}
		if raw, err := os.ReadFile("/etc/os-release"); err == nil {
			if name := parseOSRelease(string(raw)); name != "" {
				env.OS = name
			}
		}
		env.PowerSource = "not read on this platform"
	}

	env.DockerClient = dockerField(run, "version", "--format", "{{.Client.Version}}")
	env.DockerServer = dockerField(run, "version", "--format", "{{.Server.Version}}")
	if out, err := run("docker", "info", "--format", "{{.NCPU}} {{.MemTotal}}"); err == nil {
		if cpus, mem, ok := parseDockerInfo(out); ok {
			env.DockerVMCPUs, env.DockerVMMemory = cpus, mem
		}
	}
	if out, err := run("docker", "compose", "config", "--images"); err == nil {
		for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
			if line = strings.TrimSpace(line); line != "" {
				env.ComposeImages = append(env.ComposeImages, line)
			}
		}
		sort.Strings(env.ComposeImages)
	}
	return env
}

func sysctl(run commandFunc, key string) string {
	out, _ := run("sysctl", "-n", key)
	return out
}

func dockerField(run commandFunc, args ...string) string {
	out, err := run("docker", args...)
	if err != nil {
		return "unavailable: " + err.Error()
	}
	return firstLine(out, "unknown")
}

func firstLine(s, fallback string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return fallback
	}
	line, _, _ := strings.Cut(s, "\n")
	return strings.TrimSpace(line)
}

// parsePower reads `pmset -g batt`.
func parsePower(out string) string {
	switch {
	case strings.Contains(out, "'AC Power'"):
		if strings.Contains(out, "InternalBattery") {
			return "AC power"
		}
		return "AC power (no battery)"
	case strings.Contains(out, "'Battery Power'"):
		detail := ""
		for _, line := range strings.Split(out, "\n") {
			if strings.Contains(line, "InternalBattery") {
				fields := strings.Split(line, ";")
				if len(fields) >= 2 {
					pct := strings.TrimSpace(fields[0])
					if i := strings.LastIndexAny(pct, "\t "); i >= 0 {
						pct = strings.TrimSpace(pct[i:])
					}
					detail = fmt.Sprintf(" (%s, %s)", pct, strings.TrimSpace(fields[1]))
				}
			}
		}
		return "battery" + detail
	default:
		return "unknown"
	}
}

// parseLowPowerMode reads `pmset -g`.
func parseLowPowerMode(out string) string {
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "lowpowermode" {
			switch fields[1] {
			case "1":
				return "on"
			case "0":
				return "off"
			}
		}
	}
	return "unknown"
}

// parseDockerInfo reads "<ncpu> <memtotal>".
func parseDockerInfo(out string) (cpus int, memory int64, ok bool) {
	fields := strings.Fields(out)
	if len(fields) != 2 {
		return 0, 0, false
	}
	c, err1 := strconv.Atoi(fields[0])
	m, err2 := strconv.ParseInt(fields[1], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return c, m, true
}

func parseMemInfoBytes(meminfo string) uint64 {
	for _, line := range strings.Split(meminfo, "\n") {
		if rest, found := strings.CutPrefix(line, "MemTotal:"); found {
			fields := strings.Fields(rest)
			if len(fields) >= 1 {
				if kb, err := strconv.ParseUint(fields[0], 10, 64); err == nil {
					return kb * 1024
				}
			}
		}
	}
	return 0
}

func parseCPUInfoModel(cpuinfo string) string {
	for _, line := range strings.Split(cpuinfo, "\n") {
		if key, value, found := strings.Cut(line, ":"); found && strings.TrimSpace(key) == "model name" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func parseOSRelease(osRelease string) string {
	for _, line := range strings.Split(osRelease, "\n") {
		if value, found := strings.CutPrefix(line, "PRETTY_NAME="); found {
			return strings.Trim(strings.TrimSpace(value), `"`)
		}
	}
	return ""
}
