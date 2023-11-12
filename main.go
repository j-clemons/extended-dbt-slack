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

    "github.com/joho/godotenv"
    "github.com/slack-go/slack"
    "github.com/gofiber/fiber/v2"
)

type DBTRunWebhook struct {
    JobID     string
    RunStatus string
}

func parseDBTWebhook(webhook []byte) DBTRunWebhook {
    var hookJSON map[string]any

    err := json.Unmarshal(webhook, &hookJSON)
    if err != nil {
        log.Println("Error decoding webhook")
        log.Fatal(err)
    }

    jobID, ok := hookJSON["data"].(map[string]any)["jobId"]
    if ok == false {
        log.Fatal("jobId does not exist in hookJSON")
    }

    runStatus, ok := hookJSON["data"].(map[string]any)["runStatus"]
    if ok == false {
        log.Fatal("runStatus does not exist in hookJSON")
    }

    wh := DBTRunWebhook{
        JobID:     jobID.(string),
        RunStatus: runStatus.(string),
    }

    return wh
}

func getDBTRunResults(h DBTRunWebhook) string {
    account_id := os.Getenv("DBT_ACCOUNT_ID")
    url := fmt.Sprintf(
        "https://cloud.getdbt.com/api/v2/accounts/%q/runs/%q/?include_related=['run_steps']",
        account_id,
        h.JobID,
    )

    bearer := fmt.Sprintf("Bearer %q", os.Getenv("DBT_TOKEN"))

    req, err := http.NewRequest("GET", url, nil)
    req.Header.Add("Authorization", bearer)

    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        log.Fatal(err)
    }
    defer resp.Body.Close()

    // var bodyJSON map[string]any
    //
    // err = json.Unmarshal(resp.Body, &bodyJSON)
    // if err != nil {
    //     log.Fatal(err)
    // }

    // runSteps, ok := bodyJSON["data"].(map[string]any)["run_steps"]

    // return the detail string (matching CLI output)
    return ""
}

func parseLogs(logStr string) ([]string, []string) {
    r, _ := regexp.Compile(`(?:ERROR creating).*?(?:\.\w{1,})`)
    summary_lines := r.FindAllString(logStr, -1)

    details, _ := regexp.Compile(`(.*(Failure|Error) in .*\n.*\n.*)`)
    detail_lines := details.FindAllString(logStr, -1)

    return summary_lines, detail_lines
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

func cliParse() {
    t, err := io.ReadAll(os.Stdin)
    if err != nil {
        panic(err)
    }

    summary, details := parseLogs(string(t))

    ts := postMessage(formatMessages(summary))

    for i := range details {
        postMessageThread(ts, details[i])
    }
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

            // if hook.RunStatus == "Errored" {
            // }

            fmt.Printf("jobId %q, status %q", hook.JobID, hook.RunStatus)

            return c.SendStatus(200)
        })

        app.Listen(":3000")
    }
}
