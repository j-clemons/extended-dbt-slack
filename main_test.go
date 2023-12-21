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

func TestValidateWebhook(t *testing.T) {
    body := []byte(`{
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
    }`)
    auth := "947656fe520da2cec4f5b52555ca17ffa6a87aa6612a0635f39d9f0733efb7f6"

    webhookKey := "yUHFU3zLH24DjUwhl2PVX2Ygcep73lOJEsKoBkEOo4tGSoPJb9xOyyMrsmcxcUvo"

    actual := validateWebhook(body, auth, webhookKey)

    if actual != true {
        t.Errorf("Webhook validation test failed")
    }
}
