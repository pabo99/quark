package main

import (
	"compress/gzip"
	"encoding/json"
	base "github.com/omegaup/go-base"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/grader"
	"github.com/omegaup/quark/runner"
	"io"
	"math/big"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"strings"
)

var (
	validEphemeralRunFilenames = map[string]struct{}{
		"details.json": {},
		"files.zip":    {},
		"logs.txt":     {},
		"request.json": {},
		"tracing.json": {},
	}
)

func saveEphemeralRunRequest(
	ctx *grader.Context,
	runCtx *grader.RunContext,
	ephemeralRunRequest *grader.EphemeralRunRequest,
) error {
	f, err := os.OpenFile(
		path.Join(runCtx.GradeDir, "request.json.gz"),
		os.O_CREATE|os.O_WRONLY,
		0644,
	)
	if err != nil {
		ctx.Log.Error("Error opening request.json.gz file for writing", "err", err)
		return err
	}
	defer f.Close()

	// Not doing `defer zw.Close()`, because it can fail and we want to make this
	// operation fail altogether if it does.
	zw := gzip.NewWriter(f)
	if err = json.NewEncoder(zw).Encode(ephemeralRunRequest); err != nil {
		zw.Close()
		ctx.Log.Error("Error marshaling json", "err", err)
		return err
	}
	if err = zw.Close(); err != nil {
		ctx.Log.Error("Error closing gzip stream", "err", err)
		return err
	}
	return nil
}

type ephemeralRunHandler struct {
	ephemeralRunManager *grader.EphemeralRunManager
	ctx                 *grader.Context
}

func (h *ephemeralRunHandler) validateRequest(
	ephemeralRunRequest *grader.EphemeralRunRequest,
) error {
	if ephemeralRunRequest.Input.Limits == nil {
		return nil
	}
	// Silently apply some caps.
	ephemeralRunRequest.Input.Limits.TimeLimit = base.MinDuration(
		h.ctx.Config.Grader.Ephemeral.CaseTimeLimit,
		ephemeralRunRequest.Input.Limits.TimeLimit,
	)
	ephemeralRunRequest.Input.Limits.OverallWallTimeLimit = base.MinDuration(
		h.ctx.Config.Grader.Ephemeral.OverallWallTimeLimit,
		ephemeralRunRequest.Input.Limits.OverallWallTimeLimit,
	)
	ephemeralRunRequest.Input.Limits.MemoryLimit = base.MinBytes(
		h.ctx.Config.Grader.Ephemeral.MemoryLimit,
		ephemeralRunRequest.Input.Limits.MemoryLimit,
	)
	return nil
}

func (h *ephemeralRunHandler) addAndWaitForRun(
	w http.ResponseWriter,
	ephemeralRunRequest *grader.EphemeralRunRequest,
	runs *grader.Queue,
) error {
	h.ctx.Metrics.CounterAdd("grader_ephemeral_runs_total", 1)
	h.ctx.Log.Debug("Adding new run", "run", ephemeralRunRequest)
	if err := h.validateRequest(ephemeralRunRequest); err != nil {
		h.ctx.Log.Error("Invalid request", "err", err)
		w.WriteHeader(http.StatusBadRequest)
		return err
	}
	maxScore := &big.Rat{}
	for _, literalCase := range ephemeralRunRequest.Input.Cases {
		maxScore.Add(maxScore, literalCase.Weight)
	}
	inputFactory, err := common.NewLiteralInputFactory(
		ephemeralRunRequest.Input,
		h.ctx.Config.Grader.RuntimePath,
		common.LiteralPersistGrader,
	)
	if err != nil {
		inputFactoryErr := err
		h.ctx.Log.Error("Error creating input factory", "err", inputFactoryErr)
		multipartWriter := multipart.NewWriter(w)
		defer multipartWriter.Close()

		w.Header().Set("Content-Type", multipartWriter.FormDataContentType())
		w.WriteHeader(http.StatusOK)
		resultWriter, err := multipartWriter.CreateFormFile("details.json", "details.json")
		if err != nil {
			h.ctx.Log.Error("Error sending details.json", "err", err)
			return inputFactoryErr
		}
		errorString := inputFactoryErr.Error()
		fakeResult := runner.NewRunResult("CE", maxScore)
		fakeResult.CompileError = &errorString
		if err = json.NewEncoder(resultWriter).Encode(fakeResult); err != nil {
			h.ctx.Log.Error("Error sending json", "err", err)
		}
		return inputFactoryErr
	}
	input, err := h.ctx.InputManager.Add(inputFactory.Hash(), inputFactory)
	if err != nil {
		h.ctx.Log.Error("Error adding input", "err", err)
		w.WriteHeader(http.StatusBadRequest)
		return err
	}

	runCtx := grader.NewEmptyRunContext(h.ctx)
	runCtx.Run.InputHash = inputFactory.Hash()
	runCtx.Run.Language = ephemeralRunRequest.Language
	runCtx.Run.MaxScore = maxScore
	runCtx.Run.Source = ephemeralRunRequest.Source
	runCtx.Priority = grader.QueuePriorityEphemeral
	ephemeralToken, err := h.ephemeralRunManager.SetEphemeral(runCtx)
	if err != nil {
		h.ctx.Log.Error("Error making run ephemeral", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return err
	}
	committed := false
	defer func(committed *bool) {
		if *committed {
			return
		}
		if err := os.RemoveAll(runCtx.GradeDir); err != nil {
			h.ctx.Log.Error("Error cleaning up after run", "err", err)
		}
	}(&committed)

	if err = grader.AddRunContext(h.ctx, runCtx, input); err != nil {
		h.ctx.Log.Error("Failed do add run context", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return err
	}

	multipartWriter := multipart.NewWriter(w)
	defer multipartWriter.Close()

	w.Header().Set("Content-Type", multipartWriter.FormDataContentType())
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("X-OmegaUp-EphemeralToken", ephemeralToken)
	w.WriteHeader(http.StatusOK)

	// Send a field so that the reader can be notified that the run
	// has been accepted and will be queued.
	if flusher, ok := w.(http.Flusher); ok {
		multipartWriter.WriteField("status", "waiting")
		flusher.Flush()
	}

	runs.AddRun(runCtx)
	h.ctx.Log.Info("enqueued run", "run", runCtx.Run)

	// Send another field so that the reader can be notified that the run has
	// been accepted and will be queued.
	if flusher, ok := w.(http.Flusher); ok {
		multipartWriter.WriteField("status", "queueing")
		flusher.Flush()
	}

	// Wait until a runner has picked the run up, or the run has been finished.
	select {
	case <-runCtx.Running():
		if flusher, ok := w.(http.Flusher); ok {
			multipartWriter.WriteField("status", "running")
			flusher.Flush()
		}
		break
	case <-runCtx.Ready():
	}
	<-runCtx.Ready()

	// Run was successful, send all the files as part of the payload.
	filenames := []string{"logs.txt.gz", "files.zip", "details.json"}
	for _, filename := range filenames {
		fd, err := os.Open(path.Join(runCtx.GradeDir, filename))
		if err != nil {
			h.ctx.Log.Error("Error opening file", "filename", filename, "err", err)
			continue
		}
		resultWriter, err := multipartWriter.CreateFormFile(filename, filename)
		if err != nil {
			h.ctx.Log.Error("Error sending file", "filename", filename, "err", err)
			continue
		}
		if _, err = io.Copy(resultWriter, fd); err != nil {
			h.ctx.Log.Error("Error sending file", "filename", filename, "err", err)
			continue
		}
	}

	// Finally commit the run to the manager.
	if err = saveEphemeralRunRequest(h.ctx, runCtx, ephemeralRunRequest); err != nil {
		return err
	}
	h.ephemeralRunManager.Commit(runCtx)
	committed = true
	h.ctx.Log.Info("Finished running ephemeral run", "token", ephemeralToken)

	return nil
}

func (h *ephemeralRunHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.ctx.Log.Info("ephemeral run request", "path", r.URL.Path)
	tokens := strings.Split(r.URL.Path, "/")

	if len(tokens) == 5 && tokens[3] == "new" && tokens[4] == "" {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Content-Type") != "application/json" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		runs, err := h.ctx.QueueManager.Get(grader.DefaultQueueName)
		if err != nil {
			h.ctx.Log.Error("Failed to get default queue", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var ephemeralRunRequest grader.EphemeralRunRequest
		if err = json.NewDecoder(r.Body).Decode(&ephemeralRunRequest); err != nil {
			h.ctx.Log.Error("Error decoding run request", "err", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		err = h.addAndWaitForRun(w, &ephemeralRunRequest, runs)
		if err != nil {
			h.ctx.Log.Error("Failed to perform ephemeral run", "err", err)
		}
	} else if len(tokens) == 5 {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if _, ok := validEphemeralRunFilenames[tokens[4]]; !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		runDirectory, ok := h.ephemeralRunManager.Get(tokens[3])
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		filename := path.Join(runDirectory, tokens[4])
		if _, err := os.Stat(filename + ".gz"); err == nil {
			if strings.HasSuffix(filename, ".txt") {
				w.Header().Add("Content-Type", "text/plain")
			} else if strings.HasSuffix(filename, ".json") {
				w.Header().Add("Content-Type", "application/json")
			}
			filename += ".gz"
			w.Header().Add("Content-Encoding", "gzip")
		}
		http.ServeFile(w, r, filename)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func registerEphemeralHandlers(
	ctx *grader.Context,
	mux *http.ServeMux,
	ephemeralRunManager *grader.EphemeralRunManager,
) {
	ephemeralRunHandler := &ephemeralRunHandler{
		ephemeralRunManager: ephemeralRunManager,
		ctx:                 ctx,
	}
	mux.Handle("/ephemeral/run/", ephemeralRunHandler)
}
