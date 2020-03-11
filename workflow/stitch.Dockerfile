# the following docker image requires github authentication
# see https://github.com/realm/ci/packages/147854 for instructions to authenticate
FROM docker.pkg.github.com/realm/ci/mongodb-realm-test-server

RUN yum update -y \
    && yum install -y \
        curl \
        wget

RUN wget -O jq https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64
RUN chmod u+x jq
RUN curl -sL https://rpm.nodesource.com/setup_12.x | bash -
RUN yum install -y nodejs
RUN npm install mongodb-stitch-cli
RUN ln -s ./node_modules/.bin/stitch-cli stitch-cli

COPY import_app.sh /app/