FROM golang:1.12
LABEL maintainer="Mikhail Raychenko <mihailr812@gmail.com>"
WORKDIR $GOPATH/src/github.com/VKCOM/kive
#path for data files
RUN mkdir -p /tmp/kive
COPY . .
RUN go get -d -v ./...
RUN go install -v ./...
EXPOSE 8080
EXPOSE 1935
CMD ["kive"]