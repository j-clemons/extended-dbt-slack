package main

import (
    "testing"
)

func TestParseDBTWebhook(t *testing.T) {
    i := `{
        "accountId": 1,
        "webhooksID": "wsu_12345abcde",
        "eventId": "wev_2L6m5BggBw9uPNuSmtg4MUiW4Re",
        "timestamp": "2023-01-31T21:15:20.419714619Z",
        "eventType": "job.run.errored",
        "webhookName": "test",
        "data": {
            "jobId": "123",
            "jobName": "dbt Vault",
            "runId": "218726483",
            "environmentId": "1234",
            "environmentName": "dbt Vault Demo",
            "dbtVersion": "1.0.0",
            "projectName": "Snowflake Github Demo",
            "projectId": "167194",
            "runStatus": "Errored",
            "runStatusCode": 20,
            "runStatusMessage": "None",
            "runReason": "Kicked off from UI by test@test.com",
            "runStartedAt": "2023-01-31T21:14:41Z",
            "runErroredAt": "2023-01-31T21:15:20Z"
        }
    }`

    expected := DBTRunWebhook{
        RunID: "218726483",
        RunStatus: "Errored",
    }
    actual := parseDBTWebhook([]byte(i))

    getDBTRunResults(expected)

    if actual != expected {
        t.Errorf("got: %q; want: %q", actual, expected)
    }

}

func TestGetDBTRunResults(t *testing.T) {
    actual := 1
    expected := 2

    inputHook := DBTRunWebhook{
        RunID: "219793804",
        RunStatus: "Errored",
    }
    getDBTRunResults(inputHook)

    if actual != expected {
        t.Errorf("got: %q; want: %q", actual, expected)
    }
}

