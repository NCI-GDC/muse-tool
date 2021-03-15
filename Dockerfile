FROM quay.io/ncigdc/muse:1.0 AS musetool
MAINTAINER Charles Czysz <czysz@uchicago.edu>

FROM quay.io/ncigdc/python37

COPY --from=musetool /usr/local/bin/muse /usr/local/bin/

ENV BINARY=muse_tool

RUN apt-get update \
  && apt-get install -y \
  	make \
  && apt-get clean autoclean \
  && apt-get autoremove -y \
  && rm -rf /var/lib/apt/lists/*

COPY dist/ /opt/

WORKDIR /opt

RUN make init-pip \
  && ln -s /opt/bin/${BINARY} /bin/${BINARY}

ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--", "muse_tool"]

CMD ["--help"]
