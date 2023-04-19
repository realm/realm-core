ARG BASE_IMAGE=test_server-859016c167fda99bf492e53ef13322b95a9462b9-race
FROM 012067661104.dkr.ecr.eu-west-1.amazonaws.com/ci/mongodb-realm-images:$BASE_IMAGE AS stitch

FROM node:lts AS stitch-cli
RUN npm install mongodb-stitch-cli

FROM centos:7

RUN yum install -y epel-release \
 && yum install -y \
        curl \
        jq \
        python-pip \
 && yum clean all \
 && pip install yq

COPY --from=stitch /app /stitch
COPY --from=stitch-cli /node_modules/mongodb-stitch-cli/stitch-cli /usr/local/bin/

ENV PATH=/stitch/mongodb/bin:$PATH

# make sure that writable files can be written by other users
RUN mkdir -p /var/data/mongodb/db \
 && chmod -R 777 /var/data/mongodb \
 && touch /var/log/{mongodb,stitch}.log \
 && chmod 777 /var/log/{mongodb,stitch}.log \
 && chmod 777 /stitch/test_config.json

COPY run.bash /
COPY default-app /apps/default/
RUN chmod -R 777 /apps/default

ENTRYPOINT [ "bash", "/run.bash" ]

EXPOSE 9090 26000

HEALTHCHECK CMD [ "nc", "-z", "localhost", "9090" ]

LABEL org.label-schema.name="MongoDB Realm Test Server" \
      org.label-schema.vendor="MongoDB Realm" \
      org.label-schema.url="https://github.com/realm/ci/tree/master/realm/docker/mongodb-realm" \
      org.label-schema.license="" \
      org.label-schema.schema-version="1.0"
