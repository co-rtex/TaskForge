package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParsePower(t *testing.T) {
	battery := "Now drawing from 'Battery Power'\n -InternalBattery-0 (id=22478947)\t87%; discharging; (no estimate) present: true\n"
	require.Equal(t, "battery (87%, discharging)", parsePower(battery))

	ac := "Now drawing from 'AC Power'\n -InternalBattery-0 (id=22478947)\t100%; charged; 0:00 remaining present: true\n"
	require.Equal(t, "AC power", parsePower(ac))

	require.Equal(t, "AC power (no battery)", parsePower("Now drawing from 'AC Power'\n"))
	require.Equal(t, "unknown", parsePower("something else entirely"))
}

func TestParseLowPowerMode(t *testing.T) {
	require.Equal(t, "on", parseLowPowerMode("Battery Power:\n sleep                1\n lowpowermode         1\n tcpkeepalive         1\n"))
	require.Equal(t, "off", parseLowPowerMode(" lowpowermode         0\n"))
	require.Equal(t, "unknown", parseLowPowerMode(" sleep 1\n"), "a setting the platform does not report is not guessed")
	require.Equal(t, "unknown", parseLowPowerMode(""))
}

func TestParseDockerInfo(t *testing.T) {
	cpus, mem, ok := parseDockerInfo("8 4109217792\n")
	require.True(t, ok)
	require.Equal(t, 8, cpus)
	require.Equal(t, int64(4109217792), mem)

	_, _, ok = parseDockerInfo("garbage")
	require.False(t, ok)
	_, _, ok = parseDockerInfo("")
	require.False(t, ok)
}

func TestParseMemInfoAndCPUInfoAndOSRelease(t *testing.T) {
	require.Equal(t, uint64(16384000*1024), parseMemInfoBytes("MemFree: 1 kB\nMemTotal:       16384000 kB\nBuffers: 2 kB\n"))
	require.Zero(t, parseMemInfoBytes("nothing here"))

	require.Equal(t, "AMD EPYC 7763 64-Core Processor", parseCPUInfoModel("processor\t: 0\nmodel name\t: AMD EPYC 7763 64-Core Processor\nmodel name\t: AMD EPYC 7763 64-Core Processor\n"))
	require.Empty(t, parseCPUInfoModel("processor: 0"))

	require.Equal(t, "Ubuntu 24.04.1 LTS", parseOSRelease("NAME=\"Ubuntu\"\nPRETTY_NAME=\"Ubuntu 24.04.1 LTS\"\nID=ubuntu\n"))
	require.Empty(t, parseOSRelease("ID=ubuntu"))
}

// A tool that is missing or fails must not stop a run being recorded; the record
// says what it could not find instead of leaving the field blank or inventing it.
func TestCaptureEnvironment_ReportsWhatItCouldNotReadAndKeepsGoing(t *testing.T) {
	run := func(name string, args ...string) (string, error) {
		if name == "docker" {
			return "", errors.New("docker: command not found")
		}
		switch name {
		case "sysctl":
			switch args[1] {
			case "machdep.cpu.brand_string":
				return "Apple M2\n", nil
			case "hw.logicalcpu":
				return "8\n", nil
			case "hw.physicalcpu":
				return "8\n", nil
			case "hw.memsize":
				return "17179869184\n", nil
			}
		case "sw_vers":
			return "26.0\n", nil
		case "pmset":
			if len(args) == 1 {
				return " lowpowermode         0\n", nil
			}
			return "Now drawing from 'AC Power'\n", nil
		}
		return "", errors.New("unexpected")
	}

	env := captureEnvironment(run, "darwin", "16.4")

	require.Equal(t, "Apple M2", env.CPUModel)
	require.Equal(t, 8, env.LogicalCores)
	require.Equal(t, uint64(17179869184), env.MemoryBytes)
	require.Contains(t, env.OS, "macOS 26.0")
	require.Equal(t, "AC power (no battery)", env.PowerSource)
	require.Equal(t, "off", env.LowPowerMode)
	require.Equal(t, "16.4", env.PostgresVersion)
	require.NotEmpty(t, env.GoVersion)
	require.Contains(t, env.DockerServer, "unavailable")
	require.Contains(t, env.DockerServer, "command not found")
}
