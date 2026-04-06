FROM golang:1.25 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o bin/kave ./cmd/kave/main.go

FROM gcr.io/distroless/base-debian12
COPY --from=build /src/bin/kave /kave
ENTRYPOINT ["/kave"]