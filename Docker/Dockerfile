FROM ubuntu:14.04

MAINTAINER Shenglai Li <sli6@uchicago.edu>

USER root

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get dist-upgrade -y --force-yes \
    && apt-get install software-properties-common -y

RUN apt-get update && apt-get install -y --force-yes \
    default-jdk \
    gcc \
    git \
    libpq-dev \
    openjdk-7-jre-headless \
    poppler-utils \
    postgresql-client \
    python-pip \
    python-virtualenv \
    # python3-dev= \
    python3.4=3.4.3-1ubuntu1~14.04.3 \
    python3.4-minimal=3.4.3-1ubuntu1~14.04.3 \
    libpython3.4-stdlib=3.4.3-1ubuntu1~14.04.3 \
    libpython3.4-minimal=3.4.3-1ubuntu1~14.04.3 \
    r-base \
    s3cmd \
    time \
    wget \
    samtools \
    tabix 

RUN apt-get install -y --reinstall libpython3.4-stdlib


RUN adduser --disabled-password --gecos '' ubuntu && adduser ubuntu sudo && echo "ubuntu	ALL=(ALL)	NOPASSWD:ALL" >> /etc/sudoers.d/ubuntu

ENV HOME /home/ubuntu

USER ubuntu

RUN mkdir ${HOME}/tools

WORKDIR ${HOME}/tools

RUN wget https://github.com/broadinstitute/picard/releases/download/1.139/picard-tools-1.139.zip \
    && mv ${HOME}/tools/picard/src ${HOME}/ \
    && unzip picard-tools-1.139.zip \
    && mv picard-tools-1.139 picard-tools \
    && rm *.zip \
    && wget http://bioinformatics.mdanderson.org/Software/MuSE/MuSEv1.0rc_submission_c039ffa

WORKDIR ${HOME}

# more ubuntu break
USER root
RUN apt-get install python3.4-dev=3.4.3-1ubuntu1~14.04.3 -y
USER ubuntu

RUN pip install --user virtualenvwrapper \
    && /bin/bash -c "source ${HOME}/.local/bin/virtualenvwrapper.sh \
    && mkvirtualenv --python=/usr/bin/python3 p3 \
    && pip install ipython numpy pandas psycopg2 sqlalchemy \
    && echo source ${HOME}/.local/bin/virtualenvwrapper.sh >> ${HOME}/.bashrc \
    && echo source ${HOME}/.virtualenvs/p3/bin/activate >> ${HOME}/.bashrc"

RUN pip install s3cmd --user
#RUN mkdir -p ${HOME}/.ssh
#ADD github_rsa ${HOME}/.ssh/github_rsa
#USER root
#RUN chown ubuntu.ubuntu ${HOME}/.ssh/github_rsa && ls -lh ${HOME}/.ssh/github_rsa
#USER ubuntu
ENV Muse_variant_calling_pipeline 0.1
RUN git clone https://github.com/Shenglai/muse_variant_calling_pipeline.git
