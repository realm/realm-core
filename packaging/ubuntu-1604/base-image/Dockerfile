FROM alanfranz/fwd-ubuntu-xenial:latest

ARG PACKAGECLOUD_URL

RUN apt-get update -y && apt-get install -y cmake curl git ninja-build

RUN curl -s $PACKAGECLOUD_URL/script.deb.sh | bash
