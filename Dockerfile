# One image with every Kosh binary. Pick the process with the command:
#   docker run kosh api | worker | relay | dispatcher | banksim | samplemerchant | lab | sim
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ \
      ./cmd/api ./cmd/worker ./cmd/relay ./cmd/dispatcher ./cmd/banksim ./cmd/samplemerchant ./cmd/lab ./cmd/sim

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata && adduser -D -u 10001 kosh
COPY --from=build /out/ /usr/local/bin/
USER kosh
ENTRYPOINT ["/bin/sh", "-c", "exec \"$0\" \"$@\""]
CMD ["lab"]
