package runner

import (
	"archive/zip"
	"errors"
	"fmt"
	"github.com/lhchavez/quark/common"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
)

type CaseResult struct {
	Verdict  string                 `json:"verdict"`
	Name     string                 `json:"name"`
	MaxScore float64                `json:"max_score"`
	Score    float64                `json:"score"`
	Meta     map[string]RunMetadata `json:"meta"`
}

type GroupResult struct {
	Group    string       `json:"group"`
	MaxScore float64      `json:"max_score"`
	Score    float64      `json:"score"`
	Cases    []CaseResult `json:"cases"`
}

type RunResult struct {
	Verdict      string                 `json:"verdict"`
	CompileError *string                `json:"compile_error,omitempty"`
	CompileMeta  map[string]RunMetadata `json:"compile_meta"`
	Score        float64                `json:"score"`
	MaxScore     float64                `json:"max_score"`
	Time         float64                `json:"time"`
	WallTime     float64                `json:"wall_time"`
	Memory       int                    `json:"memory"`
	Groups       []GroupResult          `json:"groups"`
}

type binary struct {
	name     string
	language string
}

func Grade(
	ctx *common.Context,
	filesWriter io.Writer,
	run *common.Run,
	input common.Input,
	sandbox Sandbox,
) (*RunResult, error) {
	runResult := &RunResult{
		Verdict:  "JE",
		MaxScore: run.MaxScore,
	}
	if !sandbox.Supported() {
		return runResult, errors.New("Sandbox not supported")
	}
	runRoot := path.Join(
		ctx.Config.Runner.RuntimePath,
		"grade",
		strconv.FormatUint(run.AttemptID, 10),
	)
	if !ctx.Config.Runner.PreserveFiles {
		defer os.RemoveAll(runRoot)
	}

	ctx.Log.Info("Running", "run", run)

	binaries := []binary{
		{"Main", run.Language},
	}

	generatedFiles := make([]string, 0)

	// Setup all source files.
	mainBinPath := path.Join(runRoot, "Main", "bin")
	if err := os.MkdirAll(mainBinPath, 0755); err != nil {
		return runResult, err
	}
	mainSourceFile := path.Join(mainBinPath, fmt.Sprintf("Main.%s", run.Language))
	err := ioutil.WriteFile(mainSourceFile, []byte(run.Source), 0644)
	if err != nil {
		return runResult, err
	}

	validatorBinPath := path.Join(runRoot, "validator", "bin")
	if input.Settings().Validator.Name == "custom" {
		if err := os.MkdirAll(validatorBinPath, 0755); err != nil {
			return runResult, err
		}
		validatorLang := *input.Settings().Validator.Lang
		validatorFileName := fmt.Sprintf("validator.%s", validatorLang)
		validatorSourceFile := path.Join(validatorBinPath, validatorFileName)
		err := os.Link(path.Join(input.Path(), validatorFileName), validatorSourceFile)
		if err != nil {
			return runResult, err
		}
		binaries = append(binaries, binary{"validator", validatorLang})
	}

	runResult.CompileMeta = make(map[string]RunMetadata)

	ctx.EventCollector.Add(ctx.EventFactory.NewEvent("compile", common.EventBegin))
	for _, b := range binaries {
		binRoot := path.Join(runRoot, b.name)
		binPath := path.Join(binRoot, "bin")
		sourceFile := path.Join(binPath, fmt.Sprintf("%s.%s", b.name, b.language))

		singleCompileEvent := ctx.EventFactory.NewCompleteEvent(
			b.name,
			common.Arg{"language", b.language},
		)
		compileMeta, err := sandbox.Compile(
			ctx,
			b.language,
			[]string{sourceFile},
			binPath,
			path.Join(binRoot, "compile.out"),
			path.Join(binRoot, "compile.err"),
			path.Join(binRoot, "compile.meta"),
			b.name,
			[]string{},
		)
		ctx.EventCollector.Add(singleCompileEvent)
		generatedFiles = append(
			generatedFiles,
			path.Join(b.name, "compile.out"),
			path.Join(b.name, "compile.err"),
			path.Join(b.name, "compile.meta"),
		)

		if compileMeta != nil {
			runResult.CompileMeta[b.name] = *compileMeta
		}

		if err != nil || compileMeta.Verdict != "OK" {
			ctx.Log.Error("Compile error", "err", err, "compileMeta", compileMeta)
			runResult.Verdict = "CE"
			compileErrorFile := "compile.err"
			if b.language == "pas" {
				// Lazarus writes the output of the compile error in compile.out.
				compileErrorFile = "compile.out"
			} else {
				compileErrorFile = "compile.err"
			}
			compileError := getCompileError(path.Join(binRoot, compileErrorFile))
			runResult.CompileError = &compileError
			ctx.EventCollector.Add(ctx.EventFactory.NewEvent("compile", common.EventEnd))
			return runResult, err
		}
	}
	ctx.EventCollector.Add(ctx.EventFactory.NewEvent("compile", common.EventEnd))

	groupResults := make([]GroupResult, len(input.Settings().Cases))
	runResult.Verdict = "OK"
	wallTimeLimit := (float64)(input.Settings().Limits.OverallWallTimeLimit / 1000.0)
	ctx.EventCollector.Add(ctx.EventFactory.NewEvent("run", common.EventBegin))
	for i, group := range input.Settings().Cases {
		caseResults := make([]CaseResult, len(group.Cases))
		for j, caseData := range group.Cases {
			var runMeta *RunMetadata
			if runResult.WallTime > wallTimeLimit {
				runMeta = &RunMetadata{
					Verdict: "TLE",
				}
			} else {
				singleRunEvent := ctx.EventFactory.NewCompleteEvent(caseData.Name)
				runMeta, err = sandbox.Run(
					ctx,
					input,
					run.Language,
					mainBinPath,
					path.Join(input.Path(), "in", fmt.Sprintf("%s.in", caseData.Name)),
					path.Join(runRoot, fmt.Sprintf("%s.out", caseData.Name)),
					path.Join(runRoot, fmt.Sprintf("%s.err", caseData.Name)),
					path.Join(runRoot, fmt.Sprintf("%s.meta", caseData.Name)),
					"Main",
					nil,
					nil,
					nil,
					[]string{},
					map[string]string{},
				)
				ctx.EventCollector.Add(singleRunEvent)
				if err != nil {
					ctx.Log.Error("failed to run "+caseData.Name, "err", err)
				}
				generatedFiles = append(
					generatedFiles,
					fmt.Sprintf("%s.out", caseData.Name),
					fmt.Sprintf("%s.err", caseData.Name),
					fmt.Sprintf("%s.meta", caseData.Name),
				)
			}
			runResult.Verdict = worseVerdict(runResult.Verdict, runMeta.Verdict)
			runResult.Time += runMeta.Time
			runResult.WallTime += runMeta.WallTime
			runResult.Memory = max(runResult.Memory, runMeta.Memory)

			caseResults[j] = CaseResult{
				Name:     caseData.Name,
				MaxScore: runResult.MaxScore * caseData.Weight,
				Verdict:  runMeta.Verdict,
				Meta: map[string]RunMetadata{
					"Main": *runMeta,
				},
			}
		}
		groupResults[i] = GroupResult{
			Group:    group.Name,
			MaxScore: runResult.MaxScore * group.Weight,
			Score:    0,
			Cases:    caseResults,
		}
	}
	ctx.EventCollector.Add(ctx.EventFactory.NewEvent("run", common.EventEnd))

	// Validate outputs.
	ctx.EventCollector.Add(ctx.EventFactory.NewEvent("validate", common.EventBegin))
	for i, group := range input.Settings().Cases {
		correct := true
		score := 0.0
		for j, caseData := range group.Cases {
			caseResults := groupResults[i].Cases[j]
			if caseResults.Verdict == "OK" {
				contestantPath := path.Join(
					runRoot, fmt.Sprintf("%s.out", caseData.Name),
				)
				if input.Settings().Validator.Name == "custom" {
					originalInputFile := path.Join(
						input.Path(),
						"in",
						fmt.Sprintf("%s.in", caseData.Name),
					)
					originalOutputFile := path.Join(
						input.Path(),
						"out",
						fmt.Sprintf("%s.out", caseData.Name),
					)
					runMetaFile := path.Join(runRoot, fmt.Sprintf("%s.meta", caseData.Name))
					validateMeta, err := sandbox.Run(
						ctx,
						input,
						*input.Settings().Validator.Lang,
						validatorBinPath,
						contestantPath,
						path.Join(runRoot, "validator", fmt.Sprintf("%s.out", caseData.Name)),
						path.Join(runRoot, "validator", fmt.Sprintf("%s.err", caseData.Name)),
						path.Join(runRoot, "validator", fmt.Sprintf("%s.meta", caseData.Name)),
						"validator",
						&originalInputFile,
						&originalOutputFile,
						&runMetaFile,
						[]string{},
						map[string]string{},
					)
					if err != nil {
						ctx.Log.Error("failed to validate "+caseData.Name, "err", err)
					}
					generatedFiles = append(
						generatedFiles,
						fmt.Sprintf("validator/%s.out", caseData.Name),
						fmt.Sprintf("validator/%s.err", caseData.Name),
						fmt.Sprintf("validator/%s.meta", caseData.Name),
					)
					if validateMeta.Verdict != "OK" {
						// If the validator did not exit cleanly, assume an empty output.
						contestantPath = "/dev/null"
					} else {
						contestantPath = path.Join(
							runRoot,
							"validator",
							fmt.Sprintf("%s.out", caseData.Name),
						)
					}
				}
				contestantFd, err := os.Open(contestantPath)
				if err != nil {
					ctx.Log.Warn("Error opening file", "path", contestantPath, "err", err)
					continue
				}
				defer contestantFd.Close()
				expectedPath := path.Join(
					input.Path(), "out", fmt.Sprintf("%s.out", caseData.Name),
				)
				expectedFd, err := os.Open(expectedPath)
				if err != nil {
					ctx.Log.Warn("Error opening file", "path", expectedPath, "err", err)
					continue
				}
				defer expectedFd.Close()
				runScore, err := CalculateScore(
					&input.Settings().Validator,
					contestantFd,
					expectedFd,
				)
				if err != nil {
					ctx.Log.Debug("error comparing values", "err", err)
				}
				caseResults.Score = runResult.MaxScore * runScore * caseData.Weight
				score += runScore * caseData.Weight
				if runScore == 0 {
					correct = false
				}
				if runScore != 1 {
					runResult.Verdict = worseVerdict(runResult.Verdict, "PA")
				}
			}
		}
		if correct {
			groupResults[i].Score = runResult.MaxScore * score
			runResult.Score += groupResults[i].Score
		}
	}
	ctx.EventCollector.Add(ctx.EventFactory.NewEvent("validate", common.EventEnd))

	runResult.Groups = groupResults

	if runResult.Verdict == "PA" && runResult.Score == 0 {
		runResult.Verdict = "WA"
	} else if runResult.Verdict == "OK" {
		runResult.Verdict = "AC"
		runResult.Score = runResult.MaxScore
	}

	ctx.Log.Debug("Finished running", "results", runResult)
	uploadEvent := ctx.EventFactory.NewCompleteEvent("upload")
	defer ctx.EventCollector.Add(uploadEvent)
	if err := uploadFiles(
		ctx,
		filesWriter,
		runRoot,
		input,
		generatedFiles,
	); err != nil {
		ctx.Log.Debug("uploadFiles failed", "err", err)
		return runResult, err
	}

	return runResult, nil
}

func uploadFiles(
	ctx *common.Context,
	filesWriter io.Writer,
	runRoot string,
	input common.Input,
	files []string,
) error {
	path, err := createZipFile(runRoot, files)
	if path != "" {
		defer os.Remove(path)
	}
	if err != nil {
		return err
	}
	fd, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fd.Close()
	_, err = io.Copy(filesWriter, fd)
	return err
}

func createZipFile(runRoot string, files []string) (string, error) {
	zipFd, err := ioutil.TempFile(runRoot, ".results_zip")
	if err != nil {
		return "", err
	}
	zipPath := zipFd.Name()
	defer zipFd.Close()
	zip := zip.NewWriter(zipFd)
	for _, file := range files {
		f, err := os.Open(path.Join(runRoot, file))
		if err != nil {
			continue
		}
		defer f.Close()
		zf, err := zip.Create(file)
		if err != nil {
			return zipPath, err
		}
		if _, err := io.Copy(zf, f); err != nil {
			return zipPath, err
		}
	}
	return zipPath, zip.Close()
}

func getCompileError(errorFile string) string {
	fd, err := os.Open(errorFile)
	if err != nil {
		return err.Error()
	}
	defer fd.Close()
	bytes, err := ioutil.ReadAll(fd)
	if err != nil {
		return err.Error()
	}
	return string(bytes)
}

func worseVerdict(a, b string) string {
	verdictList := []string{
		"JE",
		"CE",
		"MLE",
		"RFE",
		"RTE",
		"TLE",
		"OLE",
		"WA",
		"PA",
		"AC",
		"OK",
	}
	idxA := sliceIndex(len(verdictList),
		func(i int) bool { return verdictList[i] == a })
	idxB := sliceIndex(len(verdictList),
		func(i int) bool { return verdictList[i] == b })
	return verdictList[min(idxA, idxB)]
}

func sliceIndex(limit int, predicate func(i int) bool) int {
	for i := 0; i < limit; i++ {
		if predicate(i) {
			return i
		}
	}
	return -1
}
