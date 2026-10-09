package fsutil

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tonistiigi/fsutil/types"
	"golang.org/x/sync/errgroup"
)

// progressCountingStream wraps a Stream and sums the Size() of every packet
// that the sender is supposed to account for in its progress callback: each
// non-terminal STAT packet and each non-empty DATA packet.
type progressCountingStream struct {
	Stream
	mu    sync.Mutex
	total int
}

func (c *progressCountingStream) SendMsg(m interface{}) error {
	if p, ok := m.(*types.Packet); ok {
		if (p.Type == types.PACKET_STAT && p.Stat != nil) || (p.Type == types.PACKET_DATA && len(p.Data) > 0) {
			c.mu.Lock()
			c.total += p.Size()
			c.mu.Unlock()
		}
	}
	return c.Stream.SendMsg(m)
}

// TestSendProgressTotal checks that the cumulative size reported to the
// sender's progressCb matches the bytes the sender actually put on the wire.
func TestSendProgressTotal(t *testing.T) {
	src := t.TempDir()
	files := map[string][]byte{
		"small":     []byte("data1"),
		"dir/empty": nil,
		// Larger than the 32KiB copy buffer so it is split into several
		// DATA packets.
		"dir/large": bytes.Repeat([]byte("x"), 100*1024+7),
	}
	for name, dt := range files {
		p := filepath.Join(src, filepath.FromSlash(name))
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, dt, 0o644))
	}

	fs, err := NewFS(src)
	require.NoError(t, err)

	var (
		mu        sync.Mutex
		lastTotal = -1
		lastCalls int
	)
	progressCb := func(total int, last bool) {
		mu.Lock()
		defer mu.Unlock()
		if last {
			lastCalls++
			lastTotal = total
		}
	}

	eg, ctx := errgroup.WithContext(context.Background())
	s1, s2 := sockPairProto(ctx)
	counting := &progressCountingStream{Stream: s1}

	eg.Go(func() error {
		defer s1.(*fakeConnProto).closeSend()
		return Send(ctx, counting, fs, progressCb, nil)
	})
	eg.Go(func() error {
		return Receive(ctx, s2, t.TempDir(), ReceiveOpt{})
	})
	require.NoError(t, eg.Wait())

	want := counting.total
	require.Positive(t, want)
	require.Equal(t, 1, lastCalls, "progressCb should be called exactly once with last=true")
	require.Equal(t, want, lastTotal, "final progress total should equal the size of STAT and DATA packets sent")
}
