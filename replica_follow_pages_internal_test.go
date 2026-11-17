package litestream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

// mustBuildPagesLTX encodes an incremental LTX file that writes the given
// page numbers (ascending) and commits at the given page count.
func mustBuildPagesLTX(tb testing.TB, minTXID, maxTXID ltx.TXID, pageSize, commit uint32, pgnos []uint32) []byte {
	tb.Helper()

	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		tb.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  pageSize,
		Commit:    commit,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Timestamp: time.Now().UnixMilli(),
	}); err != nil {
		tb.Fatal(err)
	}
	for _, pgno := range pgnos {
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, bytes.Repeat([]byte{byte(pgno)}, int(pageSize))); err != nil {
			tb.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}

func mustReadPagesFile(tb testing.TB, path string) []string {
	tb.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
}

func TestApplyLTXFile_FollowPages(t *testing.T) {
	const pageSize = 4096

	open := func(data []byte) *followTestReplicaClient {
		return &followTestReplicaClient{
			OpenLTXFileFunc: func(context.Context, int, ltx.TXID, ltx.TXID, int64, int64) (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(data)), nil
			},
		}
	}

	t.Run("RecordsPagesAndCommit", func(t *testing.T) {
		info := &ltx.FileInfo{Level: 0, MinTXID: 20, MaxTXID: 21}
		r := NewReplicaWithClient(nil, open(mustBuildPagesLTX(t, info.MinTXID, info.MaxTXID, pageSize, 12, []uint32{2, 4, 5, 6, 9})))
		f := mustCreateWritableDBFile(t)
		defer func() { _ = f.Close() }()
		r.followPagesPath = FollowPagesPath(f.Name())

		if err := r.applyLTXFile(context.Background(), f, info, pageSize); err != nil {
			t.Fatalf("applyLTXFile: %v", err)
		}

		want := []string{
			"0000000000000014 0000000000000015 0 partial",
			"0000000000000014 0000000000000015 12 ok 2,4-6,9",
		}
		if got := mustReadPagesFile(t, r.followPagesPath); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("pages file=%q, want %q", got, want)
		}
	})

	t.Run("AppendsAfterConsumerTruncate", func(t *testing.T) {
		first := &ltx.FileInfo{Level: 0, MinTXID: 20, MaxTXID: 20}
		second := &ltx.FileInfo{Level: 0, MinTXID: 21, MaxTXID: 21}
		client := &followTestReplicaClient{
			OpenLTXFileFunc: func(_ context.Context, _ int, minTXID, maxTXID ltx.TXID, _, _ int64) (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(mustBuildPagesLTX(t, minTXID, maxTXID, pageSize, 3, []uint32{uint32(minTXID) - 18}))), nil
			},
		}
		r := NewReplicaWithClient(nil, client)
		f := mustCreateWritableDBFile(t)
		defer func() { _ = f.Close() }()
		r.followPagesPath = FollowPagesPath(f.Name())

		if err := r.applyLTXFile(context.Background(), f, first, pageSize); err != nil {
			t.Fatalf("applyLTXFile: %v", err)
		}
		if err := os.Truncate(r.followPagesPath, 0); err != nil {
			t.Fatal(err)
		}
		if err := r.applyLTXFile(context.Background(), f, second, pageSize); err != nil {
			t.Fatalf("applyLTXFile: %v", err)
		}

		want := []string{
			"0000000000000015 0000000000000015 0 partial",
			"0000000000000015 0000000000000015 3 ok 3",
		}
		if got := mustReadPagesFile(t, r.followPagesPath); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("pages file=%q, want %q", got, want)
		}
	})

	t.Run("FailedApplyLeavesPartialRecordLast", func(t *testing.T) {
		info := &ltx.FileInfo{Level: 0, MinTXID: 20, MaxTXID: 20}
		data := mustBuildPagesLTX(t, info.MinTXID, info.MaxTXID, pageSize, 3, []uint32{2, 3})
		data[len(data)-1] ^= 0xFF
		r := NewReplicaWithClient(nil, open(data))
		f := mustCreateWritableDBFile(t)
		defer func() { _ = f.Close() }()
		r.followPagesPath = FollowPagesPath(f.Name())

		if err := r.applyLTXFile(context.Background(), f, info, pageSize); err == nil {
			t.Fatal("expected error")
		}

		want := []string{"0000000000000014 0000000000000014 0 partial"}
		if got := mustReadPagesFile(t, r.followPagesPath); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("pages file=%q, want %q", got, want)
		}
	})

	t.Run("UnwritablePagesFileAppliesNothing", func(t *testing.T) {
		info := &ltx.FileInfo{Level: 0, MinTXID: 20, MaxTXID: 20}
		r := NewReplicaWithClient(nil, open(mustBuildPagesLTX(t, info.MinTXID, info.MaxTXID, pageSize, 3, []uint32{2})))
		f := mustCreateWritableDBFile(t)
		defer func() { _ = f.Close() }()
		r.followPagesPath = filepath.Join(t.TempDir(), "missing", "follower.db-pages")

		if err := r.applyLTXFile(context.Background(), f, info, pageSize); err == nil {
			t.Fatal("expected error")
		}

		page := make([]byte, pageSize)
		if _, err := f.ReadAt(page, pageSize); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(page, make([]byte, pageSize)) {
			t.Fatal("page 2 was written without a pages record")
		}
	})

	t.Run("NoFileWithoutOption", func(t *testing.T) {
		info := &ltx.FileInfo{Level: 0, MinTXID: 20, MaxTXID: 20}
		r := NewReplicaWithClient(nil, open(mustBuildPagesLTX(t, info.MinTXID, info.MaxTXID, pageSize, 3, []uint32{2})))
		f := mustCreateWritableDBFile(t)
		defer func() { _ = f.Close() }()

		if err := r.applyLTXFile(context.Background(), f, info, pageSize); err != nil {
			t.Fatalf("applyLTXFile: %v", err)
		}
		if _, err := os.Stat(FollowPagesPath(f.Name())); !os.IsNotExist(err) {
			t.Fatalf("pages file stat err=%v, want not exist", err)
		}
	})
}

func TestReplica_Restore_FollowPagesRequiresFollow(t *testing.T) {
	r := NewReplicaWithClient(nil, &followTestReplicaClient{})
	err := r.Restore(context.Background(), RestoreOptions{OutputPath: filepath.Join(t.TempDir(), "db"), FollowPages: true})
	if err == nil || !strings.Contains(err.Error(), "-follow-pages") {
		t.Fatalf("err=%v, want a -follow-pages error", err)
	}
}
