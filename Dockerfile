FROM golang:1.15.8-alpine3.13 as builder

WORKDIR /app

COPY . .

RUN go get -d -v ./...
RUN go build -o closure .

FROM alpine

# Final image
COPY --from=builder /app/closure /closure
