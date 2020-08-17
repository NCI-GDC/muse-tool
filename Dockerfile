FROM quay.io/ncigdc/muse:1.0 AS musetool
LABEL maintainer="sli6@uchicago.edu"
LABEL version="1.4"
LABEL description="Multithreading `MuSE call` python wrapper."

FROM python:3.7-slim

COPY --from=musetool /usr/local/bin/muse /usr/local/bin/

COPY . /opt/

ENTRYPOINT ["python3", "/opt/multi_muse_call.py"]
