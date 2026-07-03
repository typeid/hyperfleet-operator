//go:build fleetstore

package fleetstore

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const containerImage = "postgres:16-alpine"

func setupPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	containerTool := os.Getenv("CONTAINER_TOOL")
	if containerTool == "" {
		containerTool = "podman"
	}

	port := freePort(t)
	name := fmt.Sprintf("fleetstore-test-%s", port)

	cmd := exec.Command(containerTool, "run", "-d", "--rm",
		"--name", name,
		"-e", "POSTGRES_DB=fleetstore_test",
		"-e", "POSTGRES_USER=test",
		"-e", "POSTGRES_PASSWORD=test",
		"-p", fmt.Sprintf("%s:5432", port),
		containerImage,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("start postgres container: %s: %v", string(out), err)
	}
	t.Cleanup(func() {
		_ = exec.Command(containerTool, "rm", "-f", name).Run()
	})

	dsn := fmt.Sprintf("postgres://test:test@127.0.0.1:%s/fleetstore_test?sslmode=disable", port)

	var pool *pgxpool.Pool
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		pool, err = pgxpool.New(ctx, dsn)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				break
			}
			pool.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("postgres did not become ready: %v", err)
	}
	t.Cleanup(pool.Close)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	if err := Migrate(ctx, pool, logger); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	return pool
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return fmt.Sprintf("%d", port)
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}
