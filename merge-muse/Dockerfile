FROM python:3.7-stretch
LABEL maintainer="sli6@uchicago.edu"
LABEL version="1.1"
LABEL description="Utility for merging `MuSE call` outputs."

COPY . /opt

ENTRYPOINT ['python', '/opt/merge_muse.py']
CMD ['--help']
