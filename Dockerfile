FROM golang:1.24.2 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /recogo ./cmd/recogo
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /recogo /recogo
ENV HTTP_ADDR=0.0.0.0:8080 DATA_DIR=/data
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/recogo"]
CMD ["serve"]
