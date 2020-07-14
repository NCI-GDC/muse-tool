FROM python:3.7-alpine
LABEL maintainer="sli6@uchicago.edu"
LABEL version="1.1"
LABEL description="Utility for merging `MuSE call` outputs."

COPY . /opt

RUN chmod +x /opt/merge_muse.py \
	&& ln -s /opt/merge_muse.py /bin/merge_muse

ENTRYPOINT merge_muse
CMD ['--help']
