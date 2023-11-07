package main

import (
    "fmt"
    "os"
    "time"
    "regexp"
    "io"

    "github.com/joho/godotenv"
    "github.com/slack-go/slack"
)

type logPair struct {
    Summary string
    Detail  string
}

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

func pairLogs(summaries []string, details []string) []logPair {
    var results []logPair
    for i := range summaries {
       results = append(results, logPair(summaries[i], details[i]))
    }

    return results
}

func postMessage() {
    godotenv.Load(".env")

    token := os.Getenv("SLACK_AUTH_TOKEN")
    channelID := os.Getenv("SLACK_CHANNEL_ID")

    client := slack.New(token, slack.OptionDebug(true))

    attachment := slack.Attachment{
        Pretext: "Better-dbt-bot",
        Text:    "test",
        Color:   "#36a64f",
        Fields: []slack.AttachmentField{
            {
                Title: "Date",
                Value: time.Now().String(),
            },
        },
    }

    _, timestamp, err := client.PostMessage(
        channelID,
        slack.MsgOptionAttachments(attachment),
    )

    if err != nil {
        panic(err)
    }
    fmt.Printf("Message sent at %s", timestamp)
}

func main() {

    t, err := io.ReadAll(os.Stdin)
    if err != nil {
        panic(err)
    }

    parseLogs(string(t))

}
