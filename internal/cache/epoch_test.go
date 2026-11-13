package cache

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func resetEpoch(t *testing.T) string {
	t.Helper()
	marker := filepath.Join(t.TempDir(), "paisa.db.synced")
	epochPath = func() string { return marker }
	epochMu.Lock()
	epochInit = false
	epochSeen = 0
	epochMu.Unlock()
	return marker
}

func seen() int64 {
	epochMu.Lock()
	defer epochMu.Unlock()
	return epochSeen
}

func TestClearIfStaleDetectsMarkerChange(t *testing.T) {
	marker := resetEpoch(t)

	// First call only records the starting state.
	ClearIfStale()
	assert.Equal(t, int64(0), seen())

	// A marker that appears later is a change.
	MarkSynced()
	info, err := os.Stat(marker)
	assert.NoError(t, err)
	ClearIfStale()
	assert.Equal(t, info.ModTime().UnixNano(), seen())

	// No change, no new epoch.
	before := seen()
	ClearIfStale()
	assert.Equal(t, before, seen())

	// A second sync bumps the marker again.
	time.Sleep(10 * time.Millisecond)
	MarkSynced()
	ClearIfStale()
	assert.NotEqual(t, before, seen())
}
