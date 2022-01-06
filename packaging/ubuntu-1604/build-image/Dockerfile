FROM ci/realm-core:ubuntu-1604-base

RUN apt-get update && apt-get install -y \
            libssl-dev \
            libprocps4-dev \
            chrpath

CMD ["/bin/bash", "/inside/build-package"]
