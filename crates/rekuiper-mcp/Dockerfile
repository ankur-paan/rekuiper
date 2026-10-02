FROM alpine:3.20

COPY rekuiper-mcp /usr/local/bin/rekuiper-mcp

ENTRYPOINT ["/usr/local/bin/rekuiper-mcp"]
CMD ["--server-url", "http://rekuiper-live:9081"]
