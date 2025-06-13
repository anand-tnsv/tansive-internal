package stdiorunner

import (
	"os"
	"path/filepath"

	"github.com/tansive/tansive-internal/internal/tangent/config"
)

type RunnerConfig struct {
	ScriptDir string `json:"scriptDir"`
}

var runnerConfig *RunnerConfig

func Init() {
	runnerConfig = &RunnerConfig{
		ScriptDir: config.Config().StdioRunner.ScriptDir,
	}
}

func TestInit() {
	// Override the default script directory with project root path
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	// Check if we're already in the project root by looking for go.mod
	projectRoot := wd
	for {
		if _, err := os.Stat(filepath.Join(projectRoot, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(projectRoot)
		if parent == projectRoot {
			panic("could not find project root (go.mod)")
		}
		projectRoot = parent
	}
	if runnerConfig == nil {
		runnerConfig = &RunnerConfig{}
	}

	runnerConfig.ScriptDir = filepath.Join(projectRoot, "test_scripts")
}
