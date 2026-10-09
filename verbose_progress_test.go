package fsutil

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// verboseRecorder collects the paths passed to a VerboseProgressCB, keyed by
// status.
type verboseRecorder struct {
	mu    sync.Mutex
	paths map[VerboseProgressStatus][]string
}

func newVerboseRecorder() *verboseRecorder {
	return &verboseRecorder{paths: map[VerboseProgressStatus][]string{}}
}

func (r *verboseRecorder) cb(p string, status VerboseProgressStatus, _ int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, existing := range r.paths[status] {
		if existing == p {
			return
		}
	}
	r.paths[status] = append(r.paths[status], p)
}

func (r *verboseRecorder) get(status VerboseProgressStatus) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := append([]string(nil), r.paths[status]...)
	sort.Strings(out)
	return out
}

// TestVerboseProgressPathsConsistent checks that, on each side of a transfer,
// the verbose progress callback reports a file under the same path for every
// status, and that the receiver reports the same (platform-native) path that
// it hands to NotifyHashed.
//
// On Unix filepath.FromSlash/ToSlash are no-ops so this passes. On Windows the
// merge of upstream's wire-path normalisation leaves:
//   - sender: StatusStat gets the native walk path (`dir\b`) while
//     StatusSent gets the wire path stored in s.files ("dir/b");
//   - receiver: StatusStat is called with p.Stat.Path before it is converted
//     with filepath.FromSlash ("dir/b") while StatusReceiving/Received
//     get the native r.files key via pipeNames (`dir\b`). It also fires
//     before the "unrepresentable path" check, so a rejected path is still
//     reported as stat'ed.
//
// This test cannot fail on macOS/Linux; it is red only on Windows.
func TestVerboseProgressPathsConsistent(t *testing.T) {
	src := t.TempDir()
	names := []string{"a", "dir/b", "dir/sub/c"}
	for _, name := range names {
		p := filepath.Join(src, filepath.FromSlash(name))
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte("data-"+name), 0o644))
	}
	var wantFiles []string
	for _, name := range names {
		wantFiles = append(wantFiles, filepath.FromSlash(name))
	}
	sort.Strings(wantFiles)

	fs, err := NewFS(src)
	require.NoError(t, err)

	sendRec := newVerboseRecorder()
	recvRec := newVerboseRecorder()

	var hashedMu sync.Mutex
	var hashed []string
	notifyHashed := func(kind ChangeKind, p string, fi os.FileInfo, err error) error {
		if fi != nil && fi.Mode().IsRegular() {
			hashedMu.Lock()
			hashed = append(hashed, p)
			hashedMu.Unlock()
		}
		return nil
	}

	eg, ctx := errgroup.WithContext(context.Background())
	s1, s2 := sockPairProto(ctx)
	eg.Go(func() error {
		defer s1.(*fakeConnProto).closeSend()
		return Send(ctx, s1, fs, nil, sendRec.cb)
	})
	eg.Go(func() error {
		return Receive(ctx, s2, t.TempDir(), ReceiveOpt{
			NotifyHashed:      notifyHashed,
			ContentHasher:     simpleSHA256Hasher,
			VerboseProgressCb: recvRec.cb,
		})
	})
	require.NoError(t, eg.Wait())

	sort.Strings(hashed)
	require.Equal(t, wantFiles, hashed, "NotifyHashed paths")

	// Sender: every file reported as sent must have been reported as stat'ed
	// under the identical path.
	sendStat := sendRec.get(StatusStat)
	sent := sendRec.get(StatusSent)
	require.Len(t, sent, len(names), "sender StatusSent paths: %v", sent)
	for _, p := range sent {
		require.Contains(t, sendStat, p, "sender StatusSent path %q was not reported with StatusStat", p)
	}

	// Receiver: the same, and they must match what NotifyHashed reported.
	recvStat := recvRec.get(StatusStat)
	received := recvRec.get(StatusReceived)
	receiving := recvRec.get(StatusReceiving)
	require.Equal(t, wantFiles, received, "receiver StatusReceived paths")
	require.Equal(t, wantFiles, receiving, "receiver StatusReceiving paths")
	for _, p := range received {
		require.Contains(t, recvStat, p, "receiver StatusReceived path %q was not reported with StatusStat", p)
	}
}
