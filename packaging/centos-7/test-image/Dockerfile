FROM ci/realm-core:centos-7-base

# Switch this to 1 when RPM packages are signed
RUN echo localpkg_gpgcheck=0 >> /etc/yum.conf

CMD ["/bin/bash", "/inside/test"]
