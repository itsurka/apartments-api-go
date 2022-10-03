FROM golang:1.19 as base

RUN mkdir /app
WORKDIR /app

# Copy app and packages
COPY . .
COPY docker/packages/ /usr/local/go/src/

# Download all the dependencies
RUN go get -d -v ./.

# Install the package
RUN go install -v ./.

# Build the Go app
RUN go build -o /build

# Expose port 8080 to the outside world
EXPOSE 8080

# Run the executable
CMD ["/build"]