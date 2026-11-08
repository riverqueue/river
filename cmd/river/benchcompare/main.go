// Command benchcompare compares River's Go drivers on isolated databases.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type config struct {
	Backends     []string      `json:"backends"`
	BatchSize    int           `json:"batch_size"`
	Connections  int           `json:"connections"`
	GoMaxProcs   int           `json:"gomaxprocs"`
	InsertMode   string        `json:"insert_mode"`
	Jobs         int           `json:"jobs"`
	PayloadBytes int           `json:"payload_bytes"`
	Samples      int           `json:"samples"`
	Timeout      time.Duration `json:"timeout_ns"`
	Warmups      int           `json:"warmups"`
	Workers      int           `json:"workers"`
}

type report struct {
	Build     string            `json:"build"`
	Config    config            `json:"config"`
	Error     string            `json:"error,omitempty"`
	Machine   map[string]string `json:"machine"`
	StartedAt time.Time         `json:"started_at"`
	Trials    []trial           `json:"trials"`
}

type trial struct {
	Backend             string            `json:"backend"`
	Completed           int               `json:"completed"`
	DrainJobsPerSecond  float64           `json:"drain_jobs_per_second"`
	DrainSeconds        float64           `json:"drain_seconds"`
	InsertJobsPerSecond float64           `json:"insert_jobs_per_second"`
	InsertSeconds       float64           `json:"insert_seconds"`
	LoadAfter           string            `json:"load_after"`
	LoadBefore          string            `json:"load_before"`
	Round               int               `json:"round"`
	Settings            map[string]string `json:"settings"`
	StartedAt           time.Time         `json:"started_at"`
	Warmup              bool              `json:"warmup"`
}

func main() {
	var cfg config
	backends := flag.String("backends", "mysql,postgres,sqlite", "comma-separated databases to benchmark")
	flag.IntVar(&cfg.BatchSize, "batch-size", 1000, "jobs per InsertMany call")
	flag.IntVar(&cfg.Connections, "connections", 10, "Postgres/MySQL pool limit (SQLite always uses one)")
	flag.IntVar(&cfg.GoMaxProcs, "gomaxprocs", 4, "Go CPU parallelism; database servers retain their own settings")
	flag.StringVar(&cfg.InsertMode, "insert-mode", "returning", "insertion API: returning (InsertMany) or fast (InsertManyFast, count only)")
	flag.IntVar(&cfg.Jobs, "jobs", 30000, "jobs inserted and completed per trial")
	flag.IntVar(&cfg.PayloadBytes, "payload-bytes", 256, "payload string length, excluding the JSON envelope")
	flag.IntVar(&cfg.Samples, "samples", 3, "measured rounds, each containing all selected databases")
	flag.DurationVar(&cfg.Timeout, "timeout", 5*time.Minute, "maximum time per database trial, including setup")
	flag.IntVar(&cfg.Warmups, "warmups", 1, "unmeasured rounds before collecting samples")
	flag.IntVar(&cfg.Workers, "workers", 100, "concurrent no-op workers")
	output := flag.String("output", filepath.Join(os.TempDir(), "river-bench-"+time.Now().UTC().Format("20060102T150405.000000000Z")+".json"), "new JSON results file (never overwrites an existing file)")
	flag.Parse()
	cfg.Backends = strings.Split(*backends, ",")

	if err := run(cfg, *output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(cfg config, output string) error {
	if len(cfg.Backends) == 0 {
		return errors.New("select at least one backend")
	}
	for i, backend := range cfg.Backends {
		if backend != "mysql" && backend != "postgres" && backend != "sqlite" {
			return fmt.Errorf("unknown backend %q (use mysql, postgres, or sqlite)", backend)
		}
		if slices.Contains(cfg.Backends[:i], backend) {
			return fmt.Errorf("duplicate backend %q", backend)
		}
	}
	if cfg.InsertMode != "fast" && cfg.InsertMode != "returning" {
		return errors.New("insert-mode must be fast or returning")
	}
	if cfg.BatchSize < 1 || cfg.Connections < 2 || cfg.GoMaxProcs < 1 || cfg.Jobs < 1 || cfg.PayloadBytes < 0 || cfg.Samples < 1 || cfg.Timeout <= 0 || cfg.Warmups < 0 || cfg.Workers < 1 {
		return errors.New("counts and timeout must be positive; connections >= 2; warmups and payload bytes may be zero")
	}
	// Check before creating any database objects. Rewrites below update only the
	// file owned by this invocation, preserving partial results on failure.
	file, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	runtime.GOMAXPROCS(cfg.GoMaxProcs)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	result := report{
		Config: cfg,
		Machine: map[string]string{
			"arch": runtime.GOARCH, "os": runtime.GOOS, "go": runtime.Version(),
			"cpu_count": strconv.Itoa(runtime.NumCPU()), "uname": capture("uname", "-a"),
			"cpu":          capture("sysctl", "-n", "machdep.cpu.brand_string"),
			"memory_bytes": capture("sysctl", "-n", "hw.memsize"),
			"git_commit":   capture("git", "rev-parse", "HEAD"),
			"git_status":   capture("git", "status", "--short"),
		},
		StartedAt: time.Now().UTC(),
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		result.Build = info.String()
	}
	fmt.Printf("Results: %s\n", output)
	backends := cfg.Backends
	for round := range cfg.Warmups + cfg.Samples {
		// Rotate order to distribute changing machine load across backends.
		for offset := range backends {
			backend := backends[(round+offset)%len(backends)]
			sample := trial{Backend: backend, Round: round + 1, StartedAt: time.Now().UTC(), Warmup: round < cfg.Warmups}
			sample.LoadBefore = capture("uptime")
			fmt.Printf("round %d %s (warmup=%t)\n", sample.Round, backend, sample.Warmup)
			trialCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
			err := runDatabase(trialCtx, backend, cfg, &sample)
			cancel()
			sample.LoadAfter = capture("uptime")
			result.Trials = append(result.Trials, sample)
			if err != nil {
				result.Error = fmt.Sprintf("%s round %d: %v", backend, sample.Round, err)
			}
			if saveErr := saveReport(output, result); saveErr != nil {
				return errors.Join(err, saveErr)
			}
			if err != nil {
				return errors.New(result.Error)
			}
			fmt.Printf("  insert %.0f jobs/s; drain %.0f jobs/s; verified %d completions\n", sample.InsertJobsPerSecond, sample.DrainJobsPerSecond, sample.Completed)
		}
	}
	fmt.Println("\nBackend     Insert jobs/s (median [min, max])    Drain jobs/s (median [min, max])")
	for _, backend := range backends {
		var inserts, drains []float64
		for _, sample := range result.Trials {
			if sample.Backend == backend && !sample.Warmup {
				inserts = append(inserts, sample.InsertJobsPerSecond)
				drains = append(drains, sample.DrainJobsPerSecond)
			}
		}
		fmt.Printf("%-10s  %-37s %s\n", backend, summarize(inserts), summarize(drains))
	}
	return nil
}

func capture(name string, args ...string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, name, args...).Output()
	if err != nil {
		return "unavailable"
	}
	return strings.TrimSpace(string(out))
}

func saveReport(path string, result report) error {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o600)
}

func summarize(values []float64) string {
	slices.Sort(values)
	middle := len(values) / 2
	median := values[middle]
	if len(values)%2 == 0 {
		median = (values[middle-1] + values[middle]) / 2
	}
	return fmt.Sprintf("%.0f [%.0f, %.0f]", median, values[0], values[len(values)-1])
}
