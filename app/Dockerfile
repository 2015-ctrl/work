FROM golang:1.24-bullseye AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/app main.go

FROM gcr.io/distroless/base-debian11
COPY --from=builder /bin/app /bin/app
COPY static /static
CMD ["/bin/app"]

