FROM quay.io/ncigdc/muse:1.0 AS musetool
MAINTAINER Charles Czysz <czysz@uchicago.edu>

FROM python:3.7-slim

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

ENTRYPOINT ["/bin/muse_tool"]

CMD ["--help"]
