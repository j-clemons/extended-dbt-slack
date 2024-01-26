# Extended dbt Slack
Increase the usability of dbt Slack notifications with more verbose outputs directly into Slack.

# Usage
This tool is intended to be self hosted on a VM or serverless function, and also requires the user to create a Slack app.

After hosting the application post the dbt webhooks to `[application url]/dbtrunwebhook`

## Slack App
Create a custom application in your Slack Workspace. This can be pretty barebones since it just needs permissions to post messages.

__Required Bot Token Scopes:__
- chat:write
- incoming-webhook

## Required Environment Variables
`SLACK_AUTH_TOKEN` - Bot User OAuth Token provided by Slack application that was created.

`SLACK_CHANNEL_ID` - Channel ID of the Slack channel where notifications will be sent.

`DBT_ACCOUNT_ID` - dbt Cloud account ID. Used for retrieving run details.

`DBT_AUTH_TOKEN` - dbt Cloud API token. Used for retrieving run details.

`WEBHOOK_KEY` - Key provided by dbt Cloud when setting up the webhook. Used for validating the webhook upon receipt.

## Optional Environment Variables
`NOTIFY_NON_FAILING_ERRORS` - Set to `yes` to notify for errors that do not cause the run to error. (E.g. Source Freshness)
