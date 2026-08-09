FROM nufs:latest

USER root

RUN apk add --no-cache iproute2 bash curl

RUN mkdir -p /scripts

COPY deploy/netem/scripts/netem-apply.sh /scripts/
RUN chmod +x /scripts/netem-apply.sh

ENTRYPOINT ["/scripts/netem-apply.sh"]
