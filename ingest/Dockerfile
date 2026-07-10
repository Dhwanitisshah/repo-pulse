FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/ingest .

FROM alpine:3.20
RUN adduser -D -H appuser
USER appuser
COPY --from=build /out/ingest /usr/local/bin/ingest
ENTRYPOINT ["/usr/local/bin/ingest"]
