# syntax=docker/dockerfile:1.7
FROM golang:1.27-alpine AS builder

ARG SERVICE=api
WORKDIR /src

RUN apk add --no-cache ca-certificates tzdata
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/service ./apps/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot
USER nonroot:nonroot
WORKDIR /app

COPY --from=builder /out/service /app/service

ENTRYPOINT ["/app/service"]
