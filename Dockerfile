FROM golang:1.21.4 as builder
WORKDIR /app
COPY main.go ./
COPY go.mod ./
COPY go.sum ./
RUN CGO_ENABLED=0 GOOS=linux go build -v -o extended-dbt-slack

ENTRYPOINT "./extended-dbt-slack"
