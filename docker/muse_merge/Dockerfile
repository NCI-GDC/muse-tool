FROM ubuntu:16.04
LABEL maintainer="sli6@uchicago.edu"
LABEL version="1.1"
LABEL description="Utility for merging `MuSE call` outputs."

RUN apt-get update -y \
    && apt-get install -y \
    python-dev \
    && apt-get clean

ADD MergeMuSE.py /opt/
