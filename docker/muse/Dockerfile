FROM ubuntu:16.04
LABEL maintainer="sli6@uchicago.edu"
LABEL version="MuSEv1.0rc_submission_c039ffa"
LABEL description="MuSEv1.0rc_submission_c039ffa docker"

RUN apt-get update -y && \
    apt-get install -y wget

RUN cd /opt/ && wget http://bioinformatics.mdanderson.org/Software/MuSE/MuSEv1.0rc_submission_c039ffa && chmod +x MuSEv1.0rc_submission_c039ffa

