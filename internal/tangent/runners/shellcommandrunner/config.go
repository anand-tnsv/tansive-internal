package shellcommandrunner

type RunnerConfig struct {
	ScriptDir string `json:"scriptDir"`
}

var runnerConfig *RunnerConfig

func init() {
	runnerConfig = &RunnerConfig{
		ScriptDir: "~/tansive_scripts",
	}
}
