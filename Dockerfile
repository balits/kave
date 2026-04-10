FROM golang:1.25-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o bin/kave ./cmd/kave/main.go

FROM alpine:3.19
RUN apk add --no-cache bind-tools curl
COPY --from=build /src/bin/kave /kave
ENTRYPOINT ["/kave"]