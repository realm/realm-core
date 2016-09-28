FROM alanfranz/fwd-ubuntu-xenial:latest

ARG PACKAGECLOUD_URL

RUN apt-get update -y && apt-get install -y curl

RUN curl -s $PACKAGECLOUD_URL/script.deb.sh | bash
