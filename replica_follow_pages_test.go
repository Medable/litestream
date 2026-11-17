package litestream_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

// Follow mode with FollowPages records, for every applied file, the pages it
// wrote: copying those pages from the follow file onto a copy taken before
// the apply makes the copy equal to the follow file.
func TestReplica_Restore_Follow_Pages(t *testing.T) {
	ctx := context.Background()

	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE test(id INTEGER PRIMARY KEY, value TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.ExecContext(ctx, `INSERT INTO test VALUES (1, 'initial')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	c := file.NewReplicaClient(t.TempDir())
	r := litestream.NewReplicaWithClient(db, c)
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	outputPath := t.TempDir() + "/follower.db"
	followCtx, followCancel := context.WithCancel(ctx)
	defer followCancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Restore(followCtx, litestream.RestoreOptions{
			OutputPath:     outputPath,
			Follow:         true,
			FollowInterval: 50 * time.Millisecond,
			FollowPages:    true,
		})
	}()

	waitTXID := func(after ltx.TXID) ltx.TXID {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if txid, err := litestream.ReadTXIDFile(outputPath); err == nil && txid > after {
				return txid
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("txid file did not pass %s", after)
		return 0
	}

	initialTXID := waitTXID(0)
	if _, err := os.Stat(litestream.FollowPagesPath(outputPath)); !os.IsNotExist(err) {
		t.Fatalf("pages file after the initial restore: stat err=%v, want not exist", err)
	}
	before, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := sqldb.ExecContext(ctx, `INSERT INTO test VALUES (2, 'update')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	updatedTXID := waitTXID(initialTXID)

	followCancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("follow returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("follow did not shut down within timeout")
	}

	after, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(litestream.FollowPagesPath(outputPath))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")

	pageSize := db.PageSize()
	var commit int
	var maxTXID string
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			t.Fatalf("pages record %q: want at least 4 fields", line)
		}
		if fields[3] != "ok" {
			continue
		}
		maxTXID = fields[1]
		if commit, err = strconv.Atoi(fields[2]); err != nil {
			t.Fatalf("pages record %q: %v", line, err)
		}
		if len(fields) < 5 {
			continue
		}
		for _, rng := range strings.Split(fields[4], ",") {
			lo, hi, found := strings.Cut(rng, "-")
			if !found {
				hi = lo
			}
			first, err := strconv.Atoi(lo)
			if err != nil {
				t.Fatalf("pages record %q: %v", line, err)
			}
			last, err := strconv.Atoi(hi)
			if err != nil {
				t.Fatalf("pages record %q: %v", line, err)
			}
			for pgno := first; pgno <= last; pgno++ {
				off := (pgno - 1) * pageSize
				if len(before) < off+pageSize {
					before = append(before, make([]byte, off+pageSize-len(before))...)
				}
				copy(before[off:off+pageSize], after[off:off+pageSize])
			}
		}
	}

	if fields := strings.Fields(lines[len(lines)-1]); fields[3] != "ok" {
		t.Fatalf("last pages record %q: want ok", lines[len(lines)-1])
	}
	if maxTXID != updatedTXID.String() {
		t.Fatalf("last ok record max txid=%s, want %s", maxTXID, updatedTXID)
	}
	if commit*pageSize != len(after) {
		t.Fatalf("last ok record commit=%d pages, follow file has %d", commit, len(after)/pageSize)
	}
	if string(before[:commit*pageSize]) != string(after) {
		t.Fatal("the copy with the recorded pages applied differs from the follow file")
	}
}
