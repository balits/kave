FROM golang:1.24 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/thesis ./pkg/cmd/thesis/main.go

FROM gcr.io/distroless/base-debian12
COPY --from=build /bin/thesis /bin/thesis
ENTRYPOINT ["/bin/thesis"]