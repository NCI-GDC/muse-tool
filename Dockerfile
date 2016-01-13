FROM quay.io/jeremiahsavage/cdis_base

USER root
RUN apt-get update && apt-get install -y --force-yes \
    wget

USER ubuntu
ENV HOME /home/ubuntu

ENV muse-tool 0.7b

RUN mkdir -p ${HOME}/tools/muse-tool
RUN wget http://bioinformatics.mdanderson.org/Software/MuSE/MuSEv1.0rc_submission_c039ffa \
    && chmod +x MuSEv1.0rc_submission_c039ffa \
    && mv MuSEv1.0rc_submission_c039ffa ${HOME}/tools/
ADD muse-tool ${HOME}/tools/muse-tool/
ADD setup.* ${HOME}/tools/muse-tool/

RUN /bin/bash -c "source ${HOME}/.local/bin/virtualenvwrapper.sh \
    && source ~/.virtualenvs/p3/bin/activate \
    && cd ~/tools/muse-tool \
    && pip install -e ."

WORKDIR ${HOME}
