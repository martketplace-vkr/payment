FROM golang:1.25.1-alpine AS build
WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum ./

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

ENV CGO_ENABLED=0

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go build -trimpath -ldflags="-s -w" -o /bin/main ./cmd/main.go


FROM alpine:3.20

RUN apk add --no-cache ca-certificates

COPY --from=build /bin/main /bin/main

ENTRYPOINT ["/bin/main"]
