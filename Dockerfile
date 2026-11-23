FROM golang:1.26.11-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/route-mind ./cmd/route-mind && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/migrate ./cmd/migrate

FROM gcr.io/distroless/static-debian12:nonroot AS migration
COPY --from=build /out/migrate /app/migrate
ENTRYPOINT ["/app/migrate"]
CMD ["status"]

FROM gcr.io/distroless/static-debian12:nonroot AS runtime
WORKDIR /app
COPY --from=build /out/route-mind /app/route-mind
COPY configs/providers.example.yaml /app/config.yaml
EXPOSE 8000
ENTRYPOINT ["/app/route-mind"]
CMD ["-conf", "/app/config.yaml"]
