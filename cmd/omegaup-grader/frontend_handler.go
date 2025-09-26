package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	base "github.com/omegaup/go-base"
	"github.com/omegaup/quark/broadcaster"
	"github.com/omegaup/quark/common"
	"github.com/omegaup/quark/grader"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/net/http2"
)

var (
	guidRegex = regexp.MustCompile("^[0-9a-f]{32}$")
)

type graderRunningStatus struct {
	RunnerName string `json:"name"`
	ID         int64  `json:"id"`
}

type graderStatusQueue struct {
	Running           []graderRunningStatus `json:"running"`
	RunQueueLength    int                   `json:"run_queue_length"`
	RunnerQueueLength int                   `json:"runner_queue_length"`
	Runners           []string              `json:"runners"`
}

type graderStatusResponse struct {
	Status            string            `json:"status"`
	BoadcasterSockets int               `json:"broadcaster_sockets"`
	EmbeddedRunner    bool              `json:"embedded_runner"`
	RunningQueue      graderStatusQueue `json:"queue"`
}

type runGradeRequest struct {
	RunIDs  []int64 `json:"run_ids,omitempty"`
	Rejudge bool    `json:"rejudge"`
	Debug   bool    `json:"debug"`
}

type runGradeResource struct {
	RunID    int64  `json:"run_id,omitempty"`
	Filename string `json:"filename"`
}

func updateDatabase(
	ctx *grader.Context,
	db *sql.DB,
	run *grader.RunInfo,
) {
	contestScore := base.RationalToFloat(run.Result.ContestScore)
	if run.PartialScore == 1 && contestScore != 1 {
		contestScore = 0
	}
	if run.PenaltyType == "runtime" {
		_, err := db.Exec(
			`UPDATE
				Runs
			SET
				status = 'ready', verdict = ?, runtime = ?, penalty = ?, memory = ?,
				score = ?, contest_score = ?, judged_by = ?
			WHERE
				run_id = ?;`,
			run.Result.Verdict,
			run.Result.Time*1000,
			run.Result.Time*1000,
			run.Result.Memory.Bytes(),
			base.RationalToFloat(run.Result.Score),
			contestScore,
			run.Result.JudgedBy,
			run.ID,
		)
		if err != nil {
			ctx.Log.Error("Error updating the database", "err", err, "run", run)
		}
	} else {
		_, err := db.Exec(
			`UPDATE
				Runs
			SET
				status = 'ready', verdict = ?, runtime = ?, memory = ?, score = ?,
				contest_score = ?, judged_by = ?
			WHERE
				run_id = ?;`,
			run.Result.Verdict,
			run.Result.Time*1000,
			run.Result.Memory.Bytes(),
			base.RationalToFloat(run.Result.Score),
			contestScore,
			run.Result.JudgedBy,
			run.ID,
		)
		if err != nil {
			ctx.Log.Error("Error updating the database", "err", err, "run", run)
		}
	}
}

func broadcastRun(
	ctx *grader.Context,
	db *sql.DB,
	client *http.Client,
	run *grader.RunInfo,
) error {
	message := broadcaster.Message{
		Problem: run.Run.ProblemName,
		Public:  false,
	}
	if run.ID == 0 {
		// Ephemeral run. No need to broadcast.
		return nil
	}
	if run.Contest != nil {
		message.Contest = *run.Contest
	}
	type serializedRun struct {
		User         string    `json:"username"`
		Contest      *string   `json:"contest_alias,omitempty"`
		Problemset   *int64    `json:"problemset,omitempty"`
		Problem      string    `json:"alias"`
		GUID         string    `json:"guid"`
		Runtime      float64   `json:"runtime"`
		Penalty      float64   `json:"penalty"`
		Memory       base.Byte `json:"memory"`
		Score        float64   `json:"score"`
		ContestScore float64   `json:"contest_score"`
		Status       string    `json:"status"`
		Verdict      string    `json:"verdict"`
		SubmitDelay  float64   `json:"submit_delay"`
		Time         float64   `json:"time"`
		Language     string    `json:"language"`
	}
	type runFinishedMessage struct {
		Message string        `json:"message"`
		Run     serializedRun `json:"run"`
	}
	msg := runFinishedMessage{
		Message: "/run/update/",
		Run: serializedRun{
			Contest:      run.Contest,
			Problemset:   run.Problemset,
			Problem:      run.Run.ProblemName,
			GUID:         run.GUID,
			Runtime:      run.Result.Time,
			Memory:       run.Result.Memory,
			Score:        base.RationalToFloat(run.Result.Score),
			ContestScore: base.RationalToFloat(run.Result.ContestScore),
			Status:       "ready",
			Verdict:      run.Result.Verdict,
			Language:     run.Run.Language,
			Time:         -1,
			SubmitDelay:  -1,
			Penalty:      -1,
		},
	}

	err := db.QueryRow(
		`SELECT
			i.username, r.penalty, s.submit_delay, UNIX_TIMESTAMP(r.time)
		FROM
			Runs r
		INNER JOIN
			Submissions s ON s.submission_id = r.submission_id
		INNER JOIN
			Identities i ON i.identity_id = s.identity_id
		WHERE
			r.run_id = ?;`, run.ID).Scan(
		&msg.Run.User,
		&msg.Run.Penalty,
		&msg.Run.SubmitDelay,
		&msg.Run.Time,
	)
	if err != nil {
		return err
	}
	message.User = msg.Run.User
	if run.Problemset != nil {
		message.Problemset = *run.Problemset
	}

	marshaled, err := json.Marshal(&msg)
	if err != nil {
		return err
	}

	message.Message = string(marshaled)

	if err := broadcast(ctx, client, &message); err != nil {
		ctx.Log.Error("Error sending run broadcast", "err", err)
	}
	return nil
}

func runPostProcessor(
	db *sql.DB,
	finishedRuns <-chan *grader.RunInfo,
	client *http.Client,
) {
	ctx := graderContext()
	for run := range finishedRuns {
		if run.Result.Verdict == "JE" {
			ctx.Metrics.CounterAdd("grader_runs_je", 1)
		}
		if ctx.Config.Grader.V1.UpdateDatabase {
			updateDatabase(ctx, db, run)
		}
		if ctx.Config.Grader.V1.SendBroadcast {
			if err := broadcastRun(ctx, db, client, run); err != nil {
				ctx.Log.Error("Error sending run broadcast", "err", err)
			}
		}
	}
}

func getPendingRunsBatch(ctx *grader.Context, db *sql.DB, offset, limit int) ([]int64, error) {
	// Create trace context for this database operation
	traceCtx := common.NewTraceContext("getPendingRunsBatch")
	traceCtx.AddTag("offset", fmt.Sprintf("%d", offset))
	traceCtx.AddTag("limit", fmt.Sprintf("%d", limit))
	traceCtx.AddTag("database", "mysql")
	
	// Create correlated logger
	logger := common.NewCorrelatedLogger(traceCtx)
	
	// Get advanced metrics collector for Apdex and alerting
	metricsCollector := common.GetGlobalAdvancedMetrics()
	
	timer := common.GetGlobalMetrics().StartTimer("getPendingRunsBatch")
	startTime := time.Now()
	defer func() {
		duration := timer.Stop()
		traceCtx.AddTag("duration_ms", fmt.Sprintf("%.2f", float64(duration)/float64(time.Millisecond)))
		
		// Record operation in advanced metrics system (Apdex, throughput, alerting)
		metricsCollector.RecordOperation("getPendingRunsBatch", duration, traceCtx)
		
		if duration > 1*time.Second {
			logger.Info("Slow getPendingRunsBatch detected", 
				"duration", duration, 
				"offset", offset, 
				"limit", limit,
				"apdex_score", fmt.Sprintf("%.3f", metricsCollector.GetApdexScore("getPendingRunsBatch")))
		}
	}()

	// Create context with timeout for database query
	queryCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	// Add trace to context
	queryCtx = common.ContextWithTrace(queryCtx, traceCtx)
	defer cancel()

	logger.Info("Starting database query for pending runs batch")

	rows, err := db.QueryContext(queryCtx,
		`SELECT run_id
		FROM Runs
		WHERE status != 'ready'
		ORDER BY run_id
		LIMIT ? OFFSET ?;`,
		limit, offset)
	if err != nil {
		traceCtx.AddTag("error", "database_query_failed")
		logger.Error("Database query failed", err, "offset", offset, "limit", limit)
		return nil, err
	}
	defer rows.Close()
	
	var runIds []int64
	for rows.Next() {
		var runID int64
		err = rows.Scan(&runID)
		if err != nil {
			traceCtx.AddTag("error", "row_scan_failed")
			logger.Error("Row scan failed", err)
			return nil, err
		}
		runIds = append(runIds, runID)
	}
	
	// Check for any iteration errors
	if err = rows.Err(); err != nil {
		traceCtx.AddTag("error", "rows_iteration_error")
		logger.Error("Rows iteration error", err)
		return nil, err
	}
	
	traceCtx.AddTag("retrieved_count", fmt.Sprintf("%d", len(runIds)))
	traceCtx.AddTag("status", "success")
	
	logger.Info("Database query completed successfully", 
		"retrieved_count", len(runIds),
		"offset", offset, 
		"limit", limit)
	
	return runIds, nil
}

func processPendingRunsAsync(ctx *grader.Context, db *sql.DB, runs *grader.Queue) {
	// Create master trace for the entire async processing operation
	masterTrace := common.NewTraceContext("processPendingRunsAsync")
	masterTrace.AddTag("component", "grader")
	masterTrace.AddTag("operation_type", "batch_processing")
	
	// Create correlated logger for the entire operation
	logger := common.NewCorrelatedLogger(masterTrace)
	
	// Get advanced metrics collector
	metricsCollector := common.GetGlobalAdvancedMetrics()
	
	startTime := time.Now()
	defer func() {
		totalDuration := time.Since(startTime)
		masterTrace.AddTag("total_duration_ms", fmt.Sprintf("%.2f", float64(totalDuration)/float64(time.Millisecond)))
		
		// Record async processing metrics
		metricsCollector.RecordOperation("processPendingRunsAsync", totalDuration, masterTrace)
		
		logger.Info("Async pending runs processing completed", 
			"total_duration", totalDuration,
			"apdex_score", fmt.Sprintf("%.3f", metricsCollector.GetApdexScore("processPendingRunsAsync")))
		
		// Recovery from panic
		if r := recover(); r != nil {
			masterTrace.AddTag("panic", fmt.Sprintf("%v", r))
			logger.Error("Panic in processPendingRunsAsync", fmt.Errorf("panic: %v", r))
		}
	}()

	const batchSize = 100
	const maxProcessingTime = 30 * time.Minute // Maximum time for entire processing
	const batchTimeout = 5 * time.Minute       // Timeout per batch
	
	// Create master context with overall timeout and trace
	masterCtx, masterCancel := context.WithTimeout(context.Background(), maxProcessingTime)
	masterCtx = common.ContextWithTrace(masterCtx, masterTrace)
	defer masterCancel()
	
	offset := 0
	processedTotal := 0
	errorCount := 0
	const maxErrors = 10 // Stop after too many errors
	
	logger.Info("Starting async processing of pending runs", 
		"batch_size", batchSize,
		"max_processing_time", maxProcessingTime,
		"batch_timeout", batchTimeout)
	
	for {
		// Check if master context is done
		select {
		case <-masterCtx.Done():
			masterTrace.AddTag("stop_reason", "timeout")
			masterTrace.AddTag("processed_before_timeout", fmt.Sprintf("%d", processedTotal))
			logger.Info("Async processing stopped due to timeout", 
				"processed", processedTotal,
				"time_elapsed", time.Since(startTime))
			return
		default:
		}
		
		// Create trace for this batch
		batchTrace := masterTrace.NewChildSpan("process_batch")
		batchTrace.AddTag("batch_offset", fmt.Sprintf("%d", offset))
		batchTrace.AddTag("batch_size", fmt.Sprintf("%d", batchSize))
		batchLogger := common.NewCorrelatedLogger(batchTrace)
		
		// Process batch with timing
		batchStart := time.Now()
		runIds, err := getPendingRunsBatch(ctx, db, offset, batchSize)
		
		if err != nil {
			errorCount++
			batchTrace.AddTag("error", "batch_retrieval_failed")
			batchTrace.AddTag("error_count", fmt.Sprintf("%d", errorCount))
			
			batchLogger.Error("Error getting pending runs batch", err,
				"offset", offset,
				"error_count", errorCount,
				"max_errors", maxErrors)
			
			if errorCount >= maxErrors {
				masterTrace.AddTag("stop_reason", "max_errors_exceeded")
				masterTrace.AddTag("final_error_count", fmt.Sprintf("%d", errorCount))
				logger.Error("Too many errors, stopping async processing",
					fmt.Errorf("max errors exceeded"),
					"error_count", errorCount,
					"processed", processedTotal)
				return
			}
			
			// Wait before retrying on error
			time.Sleep(5 * time.Second)
			continue
		}
		
		if len(runIds) == 0 {
			masterTrace.AddTag("stop_reason", "no_more_runs")
			masterTrace.AddTag("total_processed", fmt.Sprintf("%d", processedTotal))
			logger.Info("No more pending runs to process", "processed", processedTotal)
			break // No more runs
		}
		
		// Process this batch with timeout per run
		batchProcessed := 0
		for i, runID := range runIds {
			// Create trace for individual run processing
			runTrace := batchTrace.NewChildSpan("process_run")
			runTrace.AddTag("run_id", fmt.Sprintf("%d", runID))
			runTrace.AddTag("run_index_in_batch", fmt.Sprintf("%d", i))
			runLogger := common.NewCorrelatedLogger(runTrace)
			
			// Create context with timeout for each run processing
			runCtx, runCancel := context.WithTimeout(masterCtx, 30*time.Second)
			runCtx = common.ContextWithTrace(runCtx, runTrace)
			
			runStart := time.Now()
			runContext, err := newRunContextFromIDWithTimeout(ctx, db, runCtx, runID)
			if err != nil {
				runTrace.AddTag("error", "context_creation_failed")
				runLogger.Error("Error getting run context", err,
					"run_id", runID,
					"duration", time.Since(runStart))
				runCancel()
				continue
			}
			
			if err := injectRuns(
				ctx,
				runs,
				grader.QueuePriorityNormal,
				runContext,
			); err != nil {
				runTrace.AddTag("error", "injection_failed")
				runLogger.Error("Error injecting run", err,
					"run_id", runID,
					"duration", time.Since(runStart))
				runCancel()
				continue
			}
			
			runTrace.AddTag("status", "success")
			runTrace.AddTag("duration_ms", fmt.Sprintf("%.2f", float64(time.Since(runStart))/float64(time.Millisecond)))
			runLogger.Info("Run processed successfully", "run_id", runID, "duration", time.Since(runStart))
			
			runCancel()
			batchProcessed++
			processedTotal++
		}
		
		batchDuration := time.Since(batchStart)
		batchTrace.AddTag("batch_processed", fmt.Sprintf("%d", batchProcessed))
		batchTrace.AddTag("total_in_batch", fmt.Sprintf("%d", len(runIds)))
		batchTrace.AddTag("duration_ms", fmt.Sprintf("%.2f", float64(batchDuration)/float64(time.Millisecond)))
		batchTrace.AddTag("status", "completed")
		
		batchLogger.Info("Batch processing completed",
			"batch_size", len(runIds),
			"processed", batchProcessed,
			"duration", batchDuration,
			"total_processed", processedTotal)
		
		offset += batchSize
		
		// Progress logging every 1000 runs
		if processedTotal%1000 == 0 {
			avgTime := time.Duration(0)
			if processedTotal > 0 {
				avgTime = time.Since(startTime) / time.Duration(processedTotal)
			}
			
			progressTrace := masterTrace.NewChildSpan("progress_milestone")
			progressTrace.AddTag("milestone_runs", "1000")
			progressTrace.AddTag("total_processed", fmt.Sprintf("%d", processedTotal))
			progressTrace.AddTag("avg_time_per_run", avgTime.String())
			progressLogger := common.NewCorrelatedLogger(progressTrace)
			
			progressLogger.Info("Processed pending runs progress milestone", 
				"processed", processedTotal,
				"elapsed", time.Since(startTime),
				"avg_time_per_run", avgTime)
		}
		
		// If we got fewer results than batch size, we're done
		if len(runIds) < batchSize {
			masterTrace.AddTag("stop_reason", "batch_size_not_full")
			break
		}
		
		// Small delay to not overwhelm the database
		time.Sleep(10 * time.Millisecond)
	}
	
	masterTrace.AddTag("final_processed_count", fmt.Sprintf("%d", processedTotal))
	masterTrace.AddTag("final_error_count", fmt.Sprintf("%d", errorCount))
	masterTrace.AddTag("status", "completed")
	
	logger.Info("Finished processing pending runs successfully", 
		"total_processed", processedTotal,
		"total_errors", errorCount,
		"total_duration", time.Since(startTime))
}

// newRunContextFromIDWithTimeout creates a run context with timeout support
func newRunContextFromIDWithTimeout(ctx *grader.Context, db *sql.DB, timeoutCtx context.Context, runID int64) (*grader.RunContext, error) {
	timer := common.GetGlobalMetrics().StartTimer("newRunContextFromIDWithTimeout")
	defer func() {
		duration := timer.Stop()
		if duration > 5*time.Second {
			ctx.Log.Warn("Slow newRunContextFromIDWithTimeout detected", 
				"runId", runID, 
				"duration", duration)
		}
	}()
	
	// Check if context is already cancelled
	select {
	case <-timeoutCtx.Done():
		return nil, fmt.Errorf("context cancelled before processing run %d: %w", runID, timeoutCtx.Err())
	default:
	}
	
	// Use the existing function but with monitoring
	return newRunContextFromID(ctx, db, runID)
}

// startPendingRunsProcessor starts the background processor with proper goroutine management
func startPendingRunsProcessor(ctx *grader.Context, db *sql.DB, runs *grader.Queue) {
	// Use a sync.Once to ensure we only start one processor
	processorOnce.Do(func() {
		ctx.Log.Info("Starting pending runs processor goroutine")
		
		// Start the processor in a goroutine with proper error handling
		go func() {
			defer func() {
				if r := recover(); r != nil {
					ctx.Log.Error("Panic in pending runs processor", "panic", r)
					// Optionally restart the processor or notify administrators
				}
			}()
			
			processPendingRunsAsync(ctx, db, runs)
		}()
		
		// Register shutdown handler (if needed)
		registerProcessorShutdown()
	})
}

// Global variables for goroutine management
var (
	processorOnce     sync.Once
	processorShutdown chan struct{}
	processorDone     chan struct{}
)

// registerProcessorShutdown sets up graceful shutdown handling
func registerProcessorShutdown() {
	processorShutdown = make(chan struct{})
	processorDone = make(chan struct{})
	
	// This could be extended to hook into signal handling for graceful shutdown
}

// gradeDir gets the new-style Run ID-based path.
func gradeDir(ctx *grader.Context, runID int64) string {
	return path.Join(
		ctx.Config.Grader.V1.RuntimeGradePath,
		fmt.Sprintf("%02d/%02d/%d", runID%100, (runID%10000)/100, runID),
	)
}

func newRunContext(
	ctx *grader.Context,
	runCtx *grader.RunContext,
) (*grader.RunContext, error) {
	runCtx.GradeDir = gradeDir(ctx, runCtx.ID)
	gitProblemInfo, err := grader.GetProblemInformation(grader.GetRepositoryPath(
		ctx.Config.Grader.V1.RuntimePath,
		runCtx.Run.ProblemName,
	))
	if err != nil {
		return nil, err
	}

	runCtx.Result.MaxScore = runCtx.Run.MaxScore

	if gitProblemInfo.Settings.Slow {
		runCtx.Priority = grader.QueuePriorityLow
	} else {
		runCtx.Priority = grader.QueuePriorityNormal
	}

	return runCtx, nil
}

func newRunContextFromID(
	ctx *grader.Context,
	db *sql.DB,
	runID int64,
) (*grader.RunContext, error) {
	runCtx := grader.NewEmptyRunContext(ctx)
	runCtx.ID = runID
	var contestName sql.NullString
	var problemset sql.NullInt64
	var penaltyType sql.NullString
	var contestPoints sql.NullFloat64
	var partialScore sql.NullInt64
	err := db.QueryRow(
		`SELECT
			s.guid, c.alias, s.problemset_id, c.penalty_type, c.partial_score,
			s.language, p.alias, pp.points, r.version
		FROM
			Runs r
		INNER JOIN
			Submissions s ON s.submission_id = r.submission_id
		INNER JOIN
			Problems p ON p.problem_id = s.problem_id
		LEFT JOIN
			Problemset_Problems pp ON pp.problem_id = s.problem_id AND
			pp.problemset_id = s.problemset_id
		LEFT JOIN
			Contests c ON c.problemset_id = pp.problemset_id
		WHERE
			r.run_id = ?;`, runCtx.ID).Scan(
		&runCtx.GUID,
		&contestName,
		&problemset,
		&penaltyType,
		&partialScore,
		&runCtx.Run.Language,
		&runCtx.Run.ProblemName,
		&contestPoints,
		&runCtx.Run.InputHash,
	)
	if err != nil {
		return nil, err
	}

	if contestName.Valid {
		runCtx.Contest = &contestName.String
	}
	if problemset.Valid {
		runCtx.Problemset = &problemset.Int64
	}
	if penaltyType.Valid {
		runCtx.PenaltyType = penaltyType.String
	}
	if partialScore.Valid {
		runCtx.PartialScore = partialScore.Int64
	}
	if contestPoints.Valid {
		runCtx.Run.MaxScore = base.FloatToRational(contestPoints.Float64)
	} else {
		runCtx.Run.MaxScore = big.NewRat(1, 1)
	}
	return newRunContext(ctx, runCtx)
}

func readSource(ctx *grader.Context, runCtx *grader.RunContext) error {
	contents, err := ioutil.ReadFile(
		path.Join(
			ctx.Config.Grader.V1.RuntimePath,
			"submissions",
			runCtx.GUID[:2],
			runCtx.GUID[2:],
		),
	)
	if err != nil {
		return err
	}
	runCtx.Run.Source = string(contents)
	return nil
}

func injectRuns(
	ctx *grader.Context,
	runs *grader.Queue,
	priority grader.QueuePriority,
	runCtxs ...*grader.RunContext,
) error {
	for _, runCtx := range runCtxs {
		if err := readSource(ctx, runCtx); err != nil {
			ctx.Log.Error(
				"Error getting run source",
				"err", err,
				"runId", runCtx.ID,
			)
			return err
		}
		if runCtx.Priority == grader.QueuePriorityNormal {
			runCtx.Priority = priority
		}
		ctx.Log.Info("RunContext", "runCtx", runCtx)
		ctx.Metrics.CounterAdd("grader_runs_total", 1)
		input, err := ctx.InputManager.Add(
			runCtx.Run.InputHash,
			grader.NewInputFactory(
				runCtx.Run.ProblemName,
				&ctx.Config,
			),
		)
		if err != nil {
			ctx.Log.Error("Error getting input", "err", err, "run", runCtx)
			return err
		}
		if err = grader.AddRunContext(ctx, runCtx, input); err != nil {
			ctx.Log.Error("Error adding run context", "err", err, "runId", runCtx.ID)
			return err
		}
		runs.AddRun(runCtx)
	}
	return nil
}

func broadcast(
	ctx *grader.Context,
	client *http.Client,
	message *broadcaster.Message,
) error {
	marshaled, err := json.Marshal(message)
	if err != nil {
		return err
	}

	config := common.DefaultHTTPRetryConfig()
	resp, err := common.RetryHTTPRequest(
		ctx.Log,
		func() (*http.Response, error) {
			return client.Post(
				ctx.Config.Grader.BroadcasterURL,
				"text/json",
				bytes.NewReader(marshaled),
			)
		},
		config,
		"broadcast message",
	)

	if err != nil {
		ctx.Log.Error("Failed to broadcast message after retries", "err", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		err := fmt.Errorf("broadcast failed with status code %d", resp.StatusCode)
		ctx.Log.Error("Broadcast failed", "status_code", resp.StatusCode)
		return err
	}

	ctx.Log.Debug("Broadcast successful", "message", message)
	return nil
}

func registerFrontendHandlers(mux *http.ServeMux, db *sql.DB) {
	runs, err := graderContext().QueueManager.Get(grader.DefaultQueueName)
	if err != nil {
		panic(err)
	}
	
	// Start async processing of pending runs with proper goroutine management
	startPendingRunsProcessor(graderContext(), db, runs)

	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if !*insecure {
		cert, err := ioutil.ReadFile(graderContext().Config.TLS.CertFile)
		if err != nil {
			panic(err)
		}
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(cert)
		keyPair, err := tls.LoadX509KeyPair(
			graderContext().Config.TLS.CertFile,
			graderContext().Config.TLS.KeyFile,
		)
		transport.TLSClientConfig = &tls.Config{
			Certificates: []tls.Certificate{keyPair},
			RootCAs:      certPool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
		}
		if err != nil {
			panic(err)
		}
		if err := http2.ConfigureTransport(transport); err != nil {
			panic(err)
		}
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   60 * time.Second,
	}

	finishedRunsChan := make(chan *grader.RunInfo, 1)
	graderContext().InflightMonitor.PostProcessor.AddListener(finishedRunsChan)
	go runPostProcessor(db, finishedRunsChan, client)

	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/grader/status/", func(w http.ResponseWriter, r *http.Request) {
		ctx := graderContext()
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		runData := ctx.InflightMonitor.GetRunData()
		status := graderStatusResponse{
			Status: "ok",
			RunningQueue: graderStatusQueue{
				Runners: []string{},
				Running: make([]graderRunningStatus, len(runData)),
			},
		}

		for i, data := range runData {
			status.RunningQueue.Running[i].RunnerName = data.Runner
			status.RunningQueue.Running[i].ID = data.ID
		}
		for _, queueInfo := range ctx.QueueManager.GetQueueInfo() {
			for _, l := range queueInfo.Lengths {
				status.RunningQueue.RunQueueLength += l
			}
		}
		encoder := json.NewEncoder(w)
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		if err := encoder.Encode(&status); err != nil {
			ctx.Log.Error("Error writing /grader/status/ response", "err", err)
		}
	})

	mux.HandleFunc("/run/new/", func(w http.ResponseWriter, r *http.Request) {
		ctx := graderContext()

		if r.Method != "POST" {
			ctx.Log.Error("Invalid request", "url", r.URL.Path, "method", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		tokens := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

		if len(tokens) != 3 {
			ctx.Log.Error("Invalid request", "url", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		var runCtx *grader.RunContext
		runID, err := strconv.ParseUint(tokens[2], 10, 64)
		if err != nil {
			ctx.Log.Error("Invalid Run ID", "run id", tokens[2])
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if runCtx, err = newRunContextFromID(ctx, db, int64(runID)); err != nil {
			ctx.Log.Error(
				"/run/new/",
				"runID", runID,
				"response", "internal server error",
				"err", err,
			)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		filePath := path.Join(
			ctx.Config.Grader.V1.RuntimePath,
			"submissions",
			runCtx.GUID[:2],
			runCtx.GUID[2:],
		)
		if err := os.MkdirAll(path.Dir(filePath), 0755); err != nil {
			ctx.Log.Error(
				"/run/new/",
				"runID", runID,
				"response", "internal server error",
				"err", err,
			)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
		if err != nil {
			if os.IsExist(err) {
				ctx.Log.Info("/run/new/", "guid", runCtx.GUID, "response", "already exists")
				w.WriteHeader(http.StatusConflict)
				return
			}
			ctx.Log.Info("/run/new/", "guid", runCtx.GUID, "response", "internal server error", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer f.Close()

		io.Copy(f, r.Body)

		if err = injectRuns(ctx, runs, grader.QueuePriorityNormal, runCtx); err != nil {
			ctx.Log.Info("/run/new/", "guid", runCtx.GUID, "response", "internal server error", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		ctx.Log.Info("/run/new/", "guid", runCtx.GUID, "response", "ok")
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/run/grade/", func(w http.ResponseWriter, r *http.Request) {
		ctx := graderContext()
		decoder := json.NewDecoder(r.Body)
		defer r.Body.Close()

		var request runGradeRequest
		if err := decoder.Decode(&request); err != nil {
			ctx.Log.Error("Error receiving grade request", "err", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ctx.Log.Info("/run/grade/", "request", request)
		priority := grader.QueuePriorityNormal
		if request.Rejudge || request.Debug {
			priority = grader.QueuePriorityLow
		}
		var runCtxs []*grader.RunContext
		for _, runID := range request.RunIDs {
			runCtx, err := newRunContextFromID(ctx, db, runID)
			if err != nil {
				graderContext().Log.Error(
					"Error getting run context",
					"err", err,
					"run id", runID,
				)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			runCtxs = append(runCtxs, runCtx)
		}
		if err = injectRuns(ctx, runs, priority, runCtxs...); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		fmt.Fprintf(w, "{\"status\":\"ok\"}")
	})

	mux.HandleFunc("/submission/source/", func(w http.ResponseWriter, r *http.Request) {
		ctx := graderContext()

		if r.Method != "GET" {
			ctx.Log.Error("Invalid request", "url", r.URL.Path, "method", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		tokens := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

		if len(tokens) != 3 {
			ctx.Log.Error("Invalid request", "url", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		guid := tokens[2]

		if len(guid) != 32 || !guidRegex.MatchString(guid) {
			ctx.Log.Error("Invalid GUID", "guid", guid)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		filePath := path.Join(
			ctx.Config.Grader.V1.RuntimePath,
			"submissions",
			guid[:2],
			guid[2:],
		)
		f, err := os.Open(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				ctx.Log.Info("/run/source/", "guid", guid, "response", "not found")
				w.WriteHeader(http.StatusNotFound)
				return
			}
			ctx.Log.Info("/run/source/", "guid", guid, "response", "internal server error", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer f.Close()

		info, err := f.Stat()
		if err != nil {
			ctx.Log.Info("/run/source/", "guid", guid, "response", "internal server error", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))

		ctx.Log.Info("/run/source/", "guid", guid, "response", "ok")
		w.WriteHeader(http.StatusOK)
		io.Copy(w, f)
	})

	mux.HandleFunc("/run/resource/", func(w http.ResponseWriter, r *http.Request) {
		ctx := graderContext()
		decoder := json.NewDecoder(r.Body)
		defer r.Body.Close()

		var request runGradeResource
		if err := decoder.Decode(&request); err != nil {
			ctx.Log.Error("Error receiving resource request", "err", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if request.RunID == 0 {
			ctx.Log.Info("/run/resource/", "request", request, "response", "not found", "err", err)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if request.Filename == "" || strings.HasPrefix(request.Filename, ".") ||
			strings.Contains(request.Filename, "/") {
			ctx.Log.Error("Invalid filename", "filename", request.Filename)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		filePath := path.Join(
			gradeDir(ctx, request.RunID),
			request.Filename,
		)
		f, err := os.Open(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				ctx.Log.Info("/run/resource/", "request", request, "response", "not found", "err", err)
				w.WriteHeader(http.StatusNotFound)
				return
			}
			ctx.Log.Info("/run/resource/", "request", request, "response", "internal server error", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer f.Close()

		info, err := f.Stat()
		if err != nil {
			ctx.Log.Info("/run/resource/", "request", request, "response", "internal server error", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))

		ctx.Log.Info("/run/resource/", "request", request, "response", "ok")
		w.WriteHeader(http.StatusOK)
		io.Copy(w, f)
	})

	mux.HandleFunc("/broadcast/", func(w http.ResponseWriter, r *http.Request) {
		ctx := graderContext()
		decoder := json.NewDecoder(r.Body)
		defer r.Body.Close()

		var message broadcaster.Message
		if err := decoder.Decode(&message); err != nil {
			ctx.Log.Error("Error receiving broadcast request", "err", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ctx.Log.Debug("/broadcast/", "message", message)
		if err := broadcast(ctx, client, &message); err != nil {
			ctx.Log.Error("Error sending broadcast message", "err", err)
		}
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		fmt.Fprintf(w, "{\"status\":\"ok\"}")
	})

	mux.HandleFunc("/metrics/advanced/", func(w http.ResponseWriter, r *http.Request) {
		ctx := graderContext()
		ctx.Log.Info("/metrics/advanced/")
		
		// Get comprehensive metrics snapshot
		metricsCollector := common.GetGlobalAdvancedMetrics()
		snapshot := metricsCollector.GetMetricsSnapshot()
		
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		
		if err := json.NewEncoder(w).Encode(snapshot); err != nil {
			ctx.Log.Error("Error encoding advanced metrics", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, `{"status":"error","message":"%s"}`, err.Error())
			return
		}
	})

	mux.HandleFunc("/alerts/status/", func(w http.ResponseWriter, r *http.Request) {
		ctx := graderContext()
		ctx.Log.Info("/alerts/status/")
		
		metricsCollector := common.GetGlobalAdvancedMetrics()
		
		// Create alert status response
		alertStatus := map[string]interface{}{
			"timestamp": time.Now().UTC(),
			"total_alerts_triggered": metricsCollector.GetMetricsSnapshot()["alerts_triggered"],
			"operations": make(map[string]interface{}),
		}
		
		// Get Apdex scores for key operations
		keyOperations := []string{"getPendingRunsBatch", "processPendingRunsAsync", "run_grade", "submission_source"}
		
		for _, operation := range keyOperations {
			apdexScore := metricsCollector.GetApdexScore(operation)
			alertStatus["operations"].(map[string]interface{})[operation] = map[string]interface{}{
				"apdex_score": apdexScore,
				"alert_status": func() string {
					if apdexScore < 0.5 {
						return "CRITICAL"
					} else if apdexScore < 0.7 {
						return "WARNING"  
					} else if apdexScore < 0.9 {
						return "GOOD"
					}
					return "EXCELLENT"
				}(),
			}
		}
		
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		
		if err := json.NewEncoder(w).Encode(alertStatus); err != nil {
			ctx.Log.Error("Error encoding alert status", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	})

	mux.HandleFunc("/reload-config/", func(w http.ResponseWriter, r *http.Request) {
		ctx := graderContext()
		ctx.Log.Info("/reload-config/")
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		fmt.Fprintf(w, "{\"status\":\"ok\"}")
	})
}
