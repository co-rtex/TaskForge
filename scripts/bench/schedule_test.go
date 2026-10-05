package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The recorded run's schedule: 24 kills across the ten minutes it takes to offer
// 10,000 jobs, the first no sooner than 20 seconds in, never closer than 5 seconds.
func recordedSchedule(t *testing.T, seed int64) []Kill {
	t.Helper()
	kills, err := Schedule(seed, 24, 10*time.Minute, 20*time.Second, 5*time.Second)
	require.NoError(t, err)
	return kills
}

func TestSchedule_TheSameSeedGivesTheSameSchedule(t *testing.T) {
	require.Equal(t, recordedSchedule(t, 42), recordedSchedule(t, 42))
	require.Equal(t, recordedSchedule(t, 20261002), recordedSchedule(t, 20261002))
}

func TestSchedule_ADifferentSeedGivesADifferentSchedule(t *testing.T) {
	a, b := recordedSchedule(t, 1), recordedSchedule(t, 2)
	require.NotEqual(t, a, b)

	// Not merely a different victim draw: the times differ too, so a run cannot
	// pass this by varying one field while ignoring the seed for the other.
	differentTimes, differentPicks := 0, 0
	for i := range a {
		if a[i].At != b[i].At {
			differentTimes++
		}
		if a[i].Pick != b[i].Pick {
			differentPicks++
		}
	}
	require.Greater(t, differentTimes, len(a)/2)
	require.Greater(t, differentPicks, len(a)/2)
}

func TestSchedule_HasTheRequestedShape(t *testing.T) {
	kills := recordedSchedule(t, 7)

	require.Len(t, kills, 24)
	require.GreaterOrEqual(t, kills[0].At, 20*time.Second)
	require.Less(t, kills[len(kills)-1].At, 10*time.Minute)
	for i := 1; i < len(kills); i++ {
		require.GreaterOrEqual(t, kills[i].At-kills[i-1].At, 5*time.Second, "kills %d and %d are too close", i-1, i)
	}
}

func TestSchedule_RefusesShapesItCannotSatisfy(t *testing.T) {
	_, err := Schedule(1, 0, time.Minute, 0, time.Second)
	require.Error(t, err, "no kills")

	_, err = Schedule(1, 5, 10*time.Second, 20*time.Second, time.Second)
	require.Error(t, err, "the first kill would be after the span ends")

	_, err = Schedule(1, 30, 60*time.Second, 0, 5*time.Second)
	require.Error(t, err, "30 kills in 60s cannot be 5s apart")
}

func TestChooseVictim_IsDeterministicAndIgnoresTheOrderCandidatesArriveIn(t *testing.T) {
	sorted := []string{"w01", "w02", "w03", "w04", "w05"}
	shuffled := []string{"w04", "w01", "w05", "w03", "w02"}

	for pick := uint64(0); pick < 50; pick++ {
		a, err := ChooseVictim(pick, sorted)
		require.NoError(t, err)
		b, err := ChooseVictim(pick, shuffled)
		require.NoError(t, err)
		require.Equal(t, a, b, "pick %d", pick)
	}
}

func TestChooseVictim_EveryCandidateCanBeChosen(t *testing.T) {
	candidates := []string{"a", "b", "c"}
	seen := map[string]bool{}
	for pick := uint64(0); pick < 9; pick++ {
		v, err := ChooseVictim(pick, candidates)
		require.NoError(t, err)
		seen[v] = true
	}
	require.Len(t, seen, 3)
}

func TestChooseVictim_NoCandidatesIsAnError(t *testing.T) {
	_, err := ChooseVictim(5, nil)
	require.Error(t, err)
}
