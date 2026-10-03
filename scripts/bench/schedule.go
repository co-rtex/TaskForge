package main

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"
)

// Kill is one scheduled worker kill: when, as an offset from the start of the
// submission phase, and a random draw that picks the victim.
type Kill struct {
	At   time.Duration
	Pick uint64
}

// Schedule lays out kills kills across [first, span) from seed. The span is cut
// into kills equal slots and each kill falls at a seeded position inside its
// own slot, never closer than minGap to its neighbours, so the kills are spread
// across the run and not clustered, and still not on a grid a system could be
// tuned to.
//
// The same seed gives the same schedule: that is what makes a recorded run
// reproducible. The generator is math/rand/v2's PCG, whose output is specified
// and stable, and everything is whole milliseconds.
func Schedule(seed int64, kills int, span, first, minGap time.Duration) ([]Kill, error) {
	if kills < 1 {
		return nil, errors.New("a schedule needs at least one kill")
	}
	if span <= first {
		return nil, fmt.Errorf("the first kill (%s in) is not before the end of the span (%s)", first, span)
	}
	width := ((span - first) / time.Duration(kills)).Truncate(time.Millisecond)
	steps := int64((width - minGap) / time.Millisecond)
	if steps < 1 {
		return nil, fmt.Errorf("%d kills in %s cannot be %s apart", kills, span-first, minGap)
	}

	// Two streams from one seed: the second constant is the golden-ratio
	// increment, so seed 0 is not a degenerate pair.
	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed)^0x9e3779b97f4a7c15))
	out := make([]Kill, kills)
	for i := range out {
		jitter := time.Duration(rng.Int64N(steps)) * time.Millisecond
		out[i] = Kill{At: first + time.Duration(i)*width + jitter, Pick: rng.Uint64()}
	}
	return out, nil
}

// ChooseVictim picks one of candidates from a kill's random draw. The candidates
// are sorted first, so the choice depends on the draw and the names and never on
// the order a query happened to return them in.
func ChooseVictim(pick uint64, candidates []string) (string, error) {
	if len(candidates) == 0 {
		return "", errors.New("there is no worker to kill")
	}
	sorted := slices.Clone(candidates)
	slices.Sort(sorted)
	return sorted[pick%uint64(len(sorted))], nil
}
