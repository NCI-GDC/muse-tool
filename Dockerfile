FROM quay.io/jeremiahsavage/cdis_base

USER root
RUN apt-get update && apt-get install -y --force-yes \
    openjdk-8-jre-headless

USER ubuntu
ENV HOME /home/ubuntu

ENV muse-tool 0.4c

RUN mkdir -p ${HOME}/tools/muse-tool
wget http://bioinformatics.mdanderson.org/Software/MuSE/MuSEv1.0rc_submission_c039ffa ${HOME}/tools/
ADD muse-tool ${HOME}/tools/muse-tool/
ADD setup.* ${HOME}/tools/muse-tool/

RUN /bin/bash -c "source ${HOME}/.local/bin/virtualenvwrapper.sh \
    && source ~/.virtualenvs/p3/bin/activate \
    && cd ~/tools/muse-tool \
    && pip install -e ."

WORKDIR ${HOME}
