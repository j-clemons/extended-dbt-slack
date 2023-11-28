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

    expected := DBTRunWebhook{}
    expected.AccountID = 1
    expected.WebhooksID = "wsu_12345abcde"
    expected.EventID = "wev_2L6m5BggBw9uPNuSmtg4MUiW4Re"
    expected.Timestamp = "2023-01-31T21:15:20.419714619Z"
    expected.EventType = "job.run.errored"
    expected.WebhookName = "test"
    expected.Data.JobID = "123"
    expected.Data.JobName = "dbt Vault"
    expected.Data.RunID = "218726483"
    expected.Data.EnvironmentID = "1234"
    expected.Data.EnvironmentName = "dbt Vault Demo"
    expected.Data.DbtVersion = "1.0.0"
    expected.Data.ProjectName = "Snowflake Github Demo"
    expected.Data.ProjectID = "167194"
    expected.Data.RunStatus = "Errored"
    expected.Data.RunStatusCode = 20
    expected.Data.RunStatusMessage = "None"
    expected.Data.RunReason = "Kicked off from UI by test@test.com"
    expected.Data.RunStartedAt = "2023-01-31T21:14:41Z"
    expected.Data.RunErroredAt = "2023-01-31T21:15:20Z"

    actual := parseDBTWebhook([]byte(i))

    if actual != expected {
        t.Errorf("got: %q; want: %q", actual, expected)
    }

}

func TestGetDBTRunResults(t *testing.T) {
    actual := 1
    expected := 2

    inputHook := DBTRunWebhook{}
    inputHook.AccountID = 1
    inputHook.WebhooksID = "wsu_12345abcde"
    inputHook.EventID = "wev_2L6m5BggBw9uPNuSmtg4MUiW4Re"
    inputHook.Timestamp = "2023-01-31T21:15:20.419714619Z"
    inputHook.EventType = "job.run.errored"
    inputHook.WebhookName = "test"
    inputHook.Data.JobID = "123"
    inputHook.Data.JobName = "dbt Vault"
    inputHook.Data.RunID = "219793804"
    inputHook.Data.EnvironmentID = "1234"
    inputHook.Data.EnvironmentName = "dbt Vault Demo"
    inputHook.Data.DbtVersion = "1.0.0"
    inputHook.Data.ProjectName = "Snowflake Github Demo"
    inputHook.Data.ProjectID = "167194"
    inputHook.Data.RunStatus = "Errored"
    inputHook.Data.RunStatusCode = 20
    inputHook.Data.RunStatusMessage = "None"
    inputHook.Data.RunReason = "Kicked off from UI by test@test.com"
    inputHook.Data.RunStartedAt = "2023-01-31T21:14:41Z"
    inputHook.Data.RunErroredAt = "2023-01-31T21:15:20Z"

    getDBTRunResults(inputHook)

    if actual != expected {
        t.Errorf("got: %q; want: %q", actual, expected)
    }
}

