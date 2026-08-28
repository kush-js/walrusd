# WALrus writer service — multi-stage build.
# The Litestream VFS integration requires cgo, so the builder uses a full
# Go toolchain with a C compiler; the runtime image carries no toolchain.

FROM golang:1.26-bookworm AS build

WORKDIR /src

# Cache module downloads between builds.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# vfs build tag enables the litestream-backed VFS (cgo + mattn/go-sqlite3).
RUN CGO_ENABLED=1 go build -tags vfs -trimpath -ldflags "-s -w" \
    -o /out/walrusd ./cmd/walrusd

FROM gcr.io/distroless/base-debian12:nonroot

COPY --from=build /out/walrusd /usr/local/bin/walrusd

# Bounded ephemeral write-buffer location (spec §8); mount a tmpfs or
# emptyDir here in production.
ENV TMPDIR=/tmp \
    WALRUS_LISTEN=":8080"

EXPOSE 8080

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/walrusd"]
