FROM ubuntu:18.04
LABEL maintainer="sli6@uchicago.edu"
LABEL version="1.4"
LABEL description="Multithreading `MuSE call` python wrapper."

RUN apt update -y \
    && apt install -y \
    openjdk-8-jre-headless \
    software-properties-common \
    wget && apt-get clean

RUN add-apt-repository ppa:deadsnakes/ppa \
    && apt update -y \
    && apt install python3.7 -y

RUN cd /opt/ \
    && wget http://bioinformatics.mdanderson.org/Software/MuSE/MuSEv1.0rc_submission_c039ffa \
    && chmod +x MuSEv1.0rc_submission_c039ffa

ADD multi_muse_call_p3.py /opt/
