package main

import (
    "flag"
    "fmt"
    "os"
    "regexp"
    "log"
    "io"
    "strings"
    "encoding/json"
    "net/http"
    "net/url"

    "github.com/joho/godotenv"
    "github.com/slack-go/slack"
    "github.com/gofiber/fiber/v2"
)

type DBTRunWebhook struct {
	AccountID   int       `json:"accountId"`
	WebhooksID  string    `json:"webhooksID"`
	EventID     string    `json:"eventId"`
	Timestamp   string    `json:"timestamp"`
	EventType   string    `json:"eventType"`
	WebhookName string    `json:"webhookName"`
	Data struct {
        JobID            string    `json:"jobId"`
        JobName          string    `json:"jobName"`
        RunID            string    `json:"runId"`
        EnvironmentID    string    `json:"environmentId"`
        EnvironmentName  string    `json:"environmentName"`
        DbtVersion       string    `json:"dbtVersion"`
        ProjectName      string    `json:"projectName"`
        ProjectID        string    `json:"projectId"`
        RunStatus        string    `json:"runStatus"`
        RunStatusCode    int       `json:"runStatusCode"`
        RunStatusMessage string    `json:"runStatusMessage"`
        RunReason        string    `json:"runReason"`
        RunStartedAt     string    `json:"runStartedAt"`
        RunErroredAt     string    `json:"runErroredAt"`
    } `json:"data"`
}

type DBTRunResults struct {
	Status struct {
		Code             int    `json:"code"`
		IsSuccess        bool   `json:"is_success"`
		UserMessage      string `json:"user_message"`
		DeveloperMessage string `json:"developer_message"`
	} `json:"status"`
	Data struct {
		ID                  int    `json:"id"`
		TriggerID           int    `json:"trigger_id"`
		AccountID           int    `json:"account_id"`
		EnvironmentID       int    `json:"environment_id"`
		ProjectID           int    `json:"project_id"`
		JobDefinitionID     int    `json:"job_definition_id"`
		Status              int    `json:"status"`
		DbtVersion          string `json:"dbt_version"`
		GitBranch           string `json:"git_branch"`
		GitSha              string `json:"git_sha"`
		StatusMessage       any    `json:"status_message"`
		OwnerThreadID       any    `json:"owner_thread_id"`
		ExecutedByThreadID  string `json:"executed_by_thread_id"`
		DeferringRunID      any    `json:"deferring_run_id"`
		ArtifactsSaved      bool   `json:"artifacts_saved"`
		ArtifactS3Path      string `json:"artifact_s3_path"`
		HasDocsGenerated    bool   `json:"has_docs_generated"`
		HasSourcesGenerated bool   `json:"has_sources_generated"`
		NotificationsSent   bool   `json:"notifications_sent"`
		BlockedBy           []any  `json:"blocked_by"`
		ScribeEnabled       bool   `json:"scribe_enabled"`
		CreatedAt           string `json:"created_at"`
		UpdatedAt           string `json:"updated_at"`
		DequeuedAt          string `json:"dequeued_at"`
		StartedAt           string `json:"started_at"`
		FinishedAt          string `json:"finished_at"`
		LastCheckedAt       string `json:"last_checked_at"`
		LastHeartbeatAt     string `json:"last_heartbeat_at"`
		ShouldStartAt       string `json:"should_start_at"`
		Trigger             any    `json:"trigger"`
		Job                 any    `json:"job"`
		Environment         any    `json:"environment"`
		RunSteps            []struct {
			ID                 int    `json:"id"`
			RunID              int    `json:"run_id"`
			AccountID          int    `json:"account_id"`
			Index              int    `json:"index"`
			Status             int    `json:"status"`
			Name               string `json:"name"`
			Logs               string `json:"logs"`
			DebugLogs          string `json:"debug_logs"`
			LogLocation        string `json:"log_location"`
			LogPath            string `json:"log_path"`
			DebugLogPath       string `json:"debug_log_path"`
			LogArchiveType     string `json:"log_archive_type"`
			TruncatedDebugLogs string `json:"truncated_debug_logs"`
			CreatedAt          string `json:"created_at"`
			UpdatedAt          string `json:"updated_at"`
			StartedAt          string `json:"started_at"`
			FinishedAt         string `json:"finished_at"`
			StatusColor        string `json:"status_color"`
			StatusHumanized    string `json:"status_humanized"`
			Duration           string `json:"duration"`
			DurationHumanized  string `json:"duration_humanized"`
			RunStepCommand     any    `json:"run_step_command"`
		} `json:"run_steps"`
		StatusHumanized         string `json:"status_humanized"`
		InProgress              bool   `json:"in_progress"`
		IsComplete              bool   `json:"is_complete"`
		IsSuccess               bool   `json:"is_success"`
		IsError                 bool   `json:"is_error"`
		IsCancelled             bool   `json:"is_cancelled"`
		Duration                string `json:"duration"`
		QueuedDuration          string `json:"queued_duration"`
		RunDuration             string `json:"run_duration"`
		DurationHumanized       string `json:"duration_humanized"`
		QueuedDurationHumanized string `json:"queued_duration_humanized"`
		RunDurationHumanized    string `json:"run_duration_humanized"`
		CreatedAtHumanized      string `json:"created_at_humanized"`
		FinishedAtHumanized     string `json:"finished_at_humanized"`
		RetryingRunID           any    `json:"retrying_run_id"`
		CanRetry                bool   `json:"can_retry"`
		RetryNotSupportedReason any    `json:"retry_not_supported_reason"`
		JobID                   int    `json:"job_id"`
		IsRunning               any    `json:"is_running"`
		Href                    string `json:"href"`
		UsedRepoCache           any    `json:"used_repo_cache"`
	} `json:"data"`
}

func parseDBTWebhook(webhook []byte) DBTRunWebhook {
    log.Println("Webhook received.")

    wh := DBTRunWebhook{}
    err := json.Unmarshal(webhook, &wh)
    if err != nil {
        log.Fatalf("Unmarshal error: %q", err)
    }

    return wh
}

func getDBTRunResults(h DBTRunWebhook) {
    err := godotenv.Load(".env")
    if err != nil {
        log.Fatal(err)
    }

    account_id := os.Getenv("DBT_ACCOUNT_ID")
    endpoint := fmt.Sprintf(
        "https://cloud.getdbt.com/api/v2/accounts/%s/runs/%s/",
        account_id,
        h.Data.RunID,
    )

    bearer := fmt.Sprintf("Bearer %s", os.Getenv("DBT_AUTH_TOKEN"))

    uri, err := url.ParseRequestURI(endpoint)
    if err != nil {
        log.Fatal(err)
    }

    data := uri.Query()
    data.Set("include_related", "['run_steps']")
    uri.RawQuery = data.Encode()

    req, err := http.NewRequest(http.MethodGet, uri.String(), nil)
    req.Header.Add("Content-Type", "application/json")
    req.Header.Add("Authorization", bearer)

    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        log.Fatal(err)
    }
    defer resp.Body.Close()

    body, err := io.ReadAll(resp.Body)
    if err != nil {
        log.Fatal(err)
    }

    if resp.StatusCode != 200 {
        log.Fatal(string(body))
    }

    r := DBTRunResults{}
    err = json.Unmarshal([]byte(body), &r)
    if err != nil {
        log.Fatalf("Unmarshal error: %q", err)
    }

    summaryOut := []string{}
    summaryL1 := fmt.Sprintf(
        `
*<%s|Run #%s %s on Job "%s">*

*Environment:* %s
*Trigger:* %s
*Duration:* %s
        `,
        r.Data.Href,
        h.Data.RunID,
        h.Data.RunStatus,
        h.Data.JobName,
        h.Data.EnvironmentName,
        h.Data.RunReason,
        r.Data.DurationHumanized,
    )

    summaryOut = append(summaryOut, summaryL1)

    detailsOut := []string{}
    for _, d := range r.Data.RunSteps {
        if d.StatusHumanized == "Success" {
            stepSummary := fmt.Sprintf(
                ">:white_check_mark: %s (%s in %s)",
                d.Name,
                d.StatusHumanized,
                d.DurationHumanized,
            )
            summaryOut = append(summaryOut, stepSummary)
        } else {
            stepSummary := fmt.Sprintf(
                ">:x: %s (%s in %s)",
                d.Name,
                d.StatusHumanized,
                d.DurationHumanized,
            )
            summaryOut = append(summaryOut, stepSummary)
        }

        _, details := parseLogs(d.Logs)
        detailsOut = append(detailsOut, details...)
    }

    if len(summaryOut) > 0 {
        postMessages(summaryOut, detailsOut)
    }
}

func parseLogs(logStr string) ([]string, []string) {
    summaryLines := []string{}

    summaryRegexp := []string{
        `(?:ERROR creating).*?(?:\.\w{1,})`,
        `(?:FAIL).*?(?:\_\w{1,})`,
    }

    for sr := range summaryRegexp {
        r, _ := regexp.Compile(summaryRegexp[sr])
        summaryLines = append(summaryLines, r.FindAllString(logStr, -1)...)
    }

    details, _ := regexp.Compile(`(.*(Failure|Error) in .*\n.*\n.*)`)
    detailLines := details.FindAllString(logStr, -1)

    for i := range summaryLines {
        summaryLines[i] = stripANSIColors(summaryLines[i])
    }

    for i := range detailLines {
        detailLines[i] = stripANSIColors(detailLines[i])
    }

    return summaryLines, detailLines
}

func postMessageThread(threadTS string, detail string) {
    err := godotenv.Load(".env")
    if err != nil {
        log.Fatal(err)
    }

    token := os.Getenv("SLACK_AUTH_TOKEN")
    channelID := os.Getenv("SLACK_CHANNEL_ID")

    client := slack.New(token, slack.OptionDebug(true))

    codeBlock := "```" + detail + "```"

    preTextField := slack.NewTextBlockObject("mrkdwn", codeBlock, false, false)
    preTextSection := slack.NewSectionBlock(preTextField, nil, nil)

    _, timestamp, err := client.PostMessage(
        channelID,
        slack.MsgOptionTS(threadTS),
        slack.MsgOptionBlocks(preTextSection),
    )

    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Message sent at %s", timestamp)
}

func postMessage(msg string) string {
    err := godotenv.Load(".env")
    if err != nil {
        log.Fatal(err)
    }

    token := os.Getenv("SLACK_AUTH_TOKEN")
    channelID := os.Getenv("SLACK_CHANNEL_ID")

    client := slack.New(token, slack.OptionDebug(true))

    preTextField := slack.NewTextBlockObject("mrkdwn", msg, false, false)
    preTextSection := slack.NewSectionBlock(preTextField, nil, nil)

    _, timestamp, err := client.PostMessage(
        channelID,
        slack.MsgOptionBlocks(preTextSection),
    )

    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Message sent at %s", timestamp)

    return timestamp
}

func formatMessages(msgLines []string) string {
    return strings.Join(msgLines[:], "\n")
}

func stripANSIColors(str string) string {
    returnStr := strings.Replace(str, "\u001b[0m", "", -1)
    returnStr = strings.Replace(returnStr, "\u001b[31m", "", -1)
    returnStr = strings.Replace(returnStr, "\u001b[32m", "", -1)
    returnStr = strings.Replace(returnStr, "\u001b[33m", "", -1)

    return returnStr
}

func postMessages(summary []string, details []string) {
    ts := postMessage(formatMessages(summary))

    for i := range details {
        postMessageThread(ts, details[i])
    }
}

func cliParse() {
    t, err := io.ReadAll(os.Stdin)
    if err != nil {
        panic(err)
    }

    summary, details := parseLogs(string(t))

    postMessages(summary, details)
}

func main() {
    cliInput := flag.Bool("cli", false, "Process stdin from cli")

    flag.Parse()

    if *cliInput {
        cliParse()
    } else {
        app := fiber.New()

        app.Post("/dbtrunwebhook", func(c *fiber.Ctx) error {
            hook := parseDBTWebhook(c.Body())

            if hook.Data.RunStatus == "Errored" {
                getDBTRunResults(hook)
                return c.SendStatus(200)
            } else {
                return c.SendStatus(200)
            }
        })

        app.Listen(":3000")
    }
}
