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
            "runId": "12345",
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
        JobID: "123",
        RunStatus: "Errored",
    }
    actual := parseDBTWebhook([]byte(i))

    if actual != expected {
        t.Errorf("got: %q; want: %q", actual, expected)
    }

}
