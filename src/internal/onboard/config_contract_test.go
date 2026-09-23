package onboard

import (
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/clarkezone/herdr-distributed-mesh/src/internal/meshlocal"
)

func TestGuidedConfigurationMatchesManagedRuntimeStore(t *testing.T) {
	for _, options := range []Options{initOptions(), joinOptions()} {
		t.Run(options.Name, func(t *testing.T) {
			f := newFixture(t)
			root, err := filepath.EvalSymlinks(filepath.Dir(f.dir))
			if err != nil {
				t.Fatal(err)
			}
			f.dir = filepath.Join(root, "managed")
			f.d.Load, f.d.Save = meshlocal.Load, meshlocal.Save
			if err := Run(context.Background(), options, io.Discard, f.d); err != nil {
				t.Fatal(err)
			}
			cfg, err := meshlocal.Load(f.dir)
			if err != nil {
				t.Fatal(err)
			}
			server := options.Server
			if server != "" {
				server += ":50052"
			}
			if cfg.Coordinator != options.Coordinator || cfg.Name != options.Name || cfg.Server != server || cfg.Tailnet != options.Tailnet || cfg.HerdrExecutable != "herdr" {
				t.Fatalf("persisted configuration changed: %+v", cfg)
			}
			f.d.Running = func(string) (bool, error) { return true, nil }
			if err := Run(context.Background(), options, io.Discard, f.d); err != nil {
				t.Fatal(err)
			}
			if f.starts != 1 || f.prompts > 1 {
				t.Fatal("exact stored configuration did not resume")
			}
		})
	}
}
