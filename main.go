package main

import (
    "fmt"
    "os"
    "regexp"
    "log"
    "io"
    "strings"

    "github.com/joho/godotenv"
    "github.com/slack-go/slack"
)

func parseLogs(logStr string) ([]string, []string) {
    r, _ := regexp.Compile(`(?:ERROR creating).*?(?:\.\w{1,})`)
    summary_lines := r.FindAllString(logStr, -1)
    for i := range summary_lines {
        fmt.Println(summary_lines[i])
    }

    details, _ := regexp.Compile(`(.*(Failure|Error) in .*\n.*\n.*)`)
    detail_lines := details.FindAllString(logStr, -1)
    for i := range detail_lines {
        fmt.Println(detail_lines[i])
    }

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

func main() {

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
