FROM ubuntu:16.04
LABEL maintainer="sli6@uchicago.edu"
LABEL version="1.4"
LABEL description="Multithreading `MuSE call` python wrapper."

RUN apt-get update -y && \
    apt-get install -y python-pip python-dev wget

RUN cd /opt/ && wget http://bioinformatics.mdanderson.org/Software/MuSE/MuSEv1.0rc_submission_c039ffa && chmod +x MuSEv1.0rc_submission_c039ffa
ADD multi_muse_call.py /opt/
