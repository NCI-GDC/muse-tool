FROM ubuntu:18.04
LABEL maintainer="sli6@uchicago.edu"
LABEL version="MuSEv1.0rc_submission_c039ffa"
LABEL description="MuSEv1.0rc_submission_c039ffa docker"

RUN apt-get update -y \
	&& apt-get install -y \
	wget \
	&& apt-get clean

RUN wget -O /bin/muse http://bioinformatics.mdanderson.org/Software/MuSE/MuSEv1.0rc_submission_c039ffa \
    && chmod +x /bin/muse

ENTRYPOINT ["/bin/muse"]
