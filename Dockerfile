FROM golang:1.24 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o bin/thesis ./cmd/thesis/main.go

FROM gcr.io/distroless/base-debian12
COPY --from=build /src/bin/thesis /bin/thesis
ENTRYPOINT ["/bin/thesis"]