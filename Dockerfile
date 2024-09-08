FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /lumberjack .

FROM gcr.io/distroless/static:nonroot

COPY --from=builder /lumberjack /lumberjack

USER nonroot:nonroot

ENTRYPOINT ["/lumberjack"]
