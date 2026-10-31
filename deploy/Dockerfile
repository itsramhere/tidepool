# Tidepool node image: static binary on distroless (PLAN.md M9.6).
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN mkdir -p /out/data && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/tidepoold ./cmd/tidepoold

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tidepoold /tidepoold
COPY --from=build --chown=65532:65532 /out/data /data
# Config and rules are mounted by the compose file; data lives in a volume.
ENTRYPOINT ["/tidepoold"]
