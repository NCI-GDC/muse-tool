FROM quay.io/ncigdc/muse-tool:latest AS musetool
LABEL maintainer="sli6@uchicago.edu"
LABEL version="1.4"
LABEL description="Multithreading `MuSE call` python wrapper."

FROM python:3.7

COPY --from=musetool /bin/muse /bin/muse

COPY . /opt/

ENTRYPOINT ["python", "/opt/multi_muse_call.py"]
