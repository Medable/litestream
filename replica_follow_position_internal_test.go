package litestream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

// A gap-fill that applies one file and fails on the next reports the
// position the applied file reached.
func TestReplica_ApplyNewLTXFiles_GapFillErrorKeepsPositionReached(t *testing.T) {
	const pageSize = 4096

	applied := &ltx.FileInfo{Level: 1, MinTXID: 11, MaxTXID: 11}
	failing := &ltx.FileInfo{Level: 1, MinTXID: 12, MaxTXID: 12}
	ahead := &ltx.FileInfo{Level: 0, MinTXID: 13, MaxTXID: 13}
	client := &followTestReplicaClient{}
	client.LTXFilesFunc = func(_ context.Context, level int, _ ltx.TXID, _ bool) (ltx.FileIterator, error) {
		switch level {
		case 0:
			return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{ahead}), nil
		case 1:
			return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{applied, failing}), nil
		}
		return ltx.NewFileInfoSliceIterator(nil), nil
	}
	client.OpenLTXFileFunc = func(_ context.Context, _ int, minTXID, maxTXID ltx.TXID, _, _ int64) (io.ReadCloser, error) {
		if minTXID == failing.MinTXID {
			return nil, errors.New("open refused")
		}
		return io.NopCloser(bytes.NewReader(mustBuildIncrementalLTX(t, minTXID, maxTXID, pageSize, 2, 0xA1))), nil
	}

	r := NewReplicaWithClient(nil, client)
	f := mustCreateWritableDBFile(t)
	defer func() { _ = f.Close() }()

	got, err := r.applyNewLTXFiles(context.Background(), f, 10, pageSize)
	if err == nil {
		t.Fatal("expected error")
	}
	if got != 11 {
		t.Fatalf("txid=%s, want %s", got, ltx.TXID(11))
	}
}

// A poll that applies one file and fails on the next writes the position
// the applied file reached, and later polls do not apply that file again.
func TestReplica_Follow_ErrorKeepsPositionReached(t *testing.T) {
	const pageSize = 4096

	applied := &ltx.FileInfo{Level: 0, MinTXID: 11, MaxTXID: 11}
	failing := &ltx.FileInfo{Level: 0, MinTXID: 12, MaxTXID: 12}
	var appliedOpens, failingOpens atomic.Int64
	client := &followTestReplicaClient{}
	client.LTXFilesFunc = func(_ context.Context, level int, seek ltx.TXID, _ bool) (ltx.FileIterator, error) {
		var infos []*ltx.FileInfo
		if level == 0 {
			for _, info := range []*ltx.FileInfo{applied, failing} {
				if info.MinTXID >= seek {
					infos = append(infos, info)
				}
			}
		}
		return ltx.NewFileInfoSliceIterator(infos), nil
	}
	client.OpenLTXFileFunc = func(_ context.Context, _ int, minTXID, maxTXID ltx.TXID, _, _ int64) (io.ReadCloser, error) {
		if minTXID == failing.MinTXID {
			failingOpens.Add(1)
			return nil, errors.New("open refused")
		}
		appliedOpens.Add(1)
		return io.NopCloser(bytes.NewReader(mustBuildIncrementalLTX(t, minTXID, maxTXID, pageSize, 2, 0xA1))), nil
	}

	outputPath := filepath.Join(t.TempDir(), "follower.db")
	if err := os.WriteFile(outputPath, append(newSQLiteHeaderPage(pageSize), make([]byte, pageSize)...), 0o600); err != nil {
		t.Fatal(err)
	}

	r := NewReplicaWithClient(nil, client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- r.follow(ctx, outputPath, 10, 10*time.Millisecond) }()

	deadline := time.Now().Add(5 * time.Second)
	for failingOpens.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("follow: %v", err)
	}

	if n := failingOpens.Load(); n < 3 {
		t.Fatalf("failing file opened %d times, want at least 3 polls", n)
	}
	if n := appliedOpens.Load(); n != 1 {
		t.Fatalf("applied file opened %d times, want 1", n)
	}
	txid, err := ReadTXIDFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if txid != 11 {
		t.Fatalf("txid file=%s, want %s", txid, ltx.TXID(11))
	}
}
