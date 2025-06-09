package shellcommandrunner

import (
	"os"
	"path/filepath"
)

type RunnerConfig struct {
	ScriptDir string `json:"scriptDir"`
}

var runnerConfig *RunnerConfig

func init() {
	runnerConfig = &RunnerConfig{
		ScriptDir: "~/tansive_scripts",
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

	runnerConfig.ScriptDir = filepath.Join(projectRoot, "test_scripts")
}
