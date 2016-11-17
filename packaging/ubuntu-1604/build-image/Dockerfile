FROM ci/realm-core:ubuntu-1604-base

RUN apt-get update && apt-get install -y \
            python-cheetah \
            libssl-dev \
            chrpath

CMD ["/bin/bash", "/inside/build-package"]
