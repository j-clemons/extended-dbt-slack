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
    RunID     string
    RunStatus string
}

func parseDBTWebhook(webhook []byte) DBTRunWebhook {
    var hookJSON map[string]any

    err := json.Unmarshal(webhook, &hookJSON)
    if err != nil {
        log.Println("Error decoding webhook")
        log.Fatal(err)
    }

    runID, ok := hookJSON["data"].(map[string]any)["runId"]
    if ok == false {
        log.Fatal("runId does not exist in hookJSON")
    }

    runStatus, ok := hookJSON["data"].(map[string]any)["runStatus"]
    if ok == false {
        log.Fatal("runStatus does not exist in hookJSON")
    }

    wh := DBTRunWebhook{
        RunID:     runID.(string),
        RunStatus: runStatus.(string),
    }

    return wh
}

func getDBTRunResults(h DBTRunWebhook) string {
    err := godotenv.Load(".env")
    if err != nil {
        log.Fatal(err)
    }

    account_id := os.Getenv("DBT_ACCOUNT_ID")
    endpoint := fmt.Sprintf(
        "https://cloud.getdbt.com/api/v2/accounts/%s/runs/%s/",
        account_id,
        h.RunID,
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

    var bodyJSON map[string]any

    body, err := io.ReadAll(resp.Body)
    if err != nil {
        log.Fatal(err)
        return ""
    }

    if resp.StatusCode != 200 {
        log.Fatal(string(body))
    }

    err = json.Unmarshal([]byte(body), &bodyJSON)
    if err != nil {
        log.Fatal(err)
    }

    runSteps, ok := bodyJSON["data"].(map[string]any)["run_steps"]
    if ok == false {
        log.Fatal("run_steps does not exist in response")
        return ""
    }

    fmt.Print(runSteps)
    // for i := range runSteps {
    //
    // }

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

            fmt.Printf("runId %q, status %q", hook.RunID, hook.RunStatus)

            return c.SendStatus(200)
        })

        app.Listen(":3000")
    }
}
