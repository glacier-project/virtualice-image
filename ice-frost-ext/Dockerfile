# Frost gadget line (GadgetMain + KafkaBridge) on the local stack. Build context: the repository root
# (GadgetMain uses ../ice-frost, ../kafka, ../ros_bridge, ../utils). Service frost-ext of docker-compose.yml.
FROM python:3.12-slim-bookworm
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN apt-get update && apt-get install -y --no-install-recommends curl git gcc g++ make cmake openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
ARG LFC_VERSION=0.13.0
RUN curl -fsSL https://github.com/lf-lang/lingua-franca/releases/download/v${LFC_VERSION}/lf-cli-${LFC_VERSION}-Linux-x86_64.tar.gz \
    | tar xz -C /opt && ln -s /opt/lf-cli-${LFC_VERSION}-Linux-x86_64/bin/lfc /usr/local/bin/lfc

WORKDIR /app
COPY ice-frost/requirements.txt ice-frost/
COPY ice-frost-ext/requirements.txt ice-frost-ext/
RUN pip install -r ice-frost-ext/requirements.txt

COPY ice-frost ice-frost
COPY kafka kafka
COPY ros_bridge ros_bridge
COPY utils utils
COPY ice-frost-ext ice-frost-ext
# lf-packages is a symlink: a plain text file in a Windows checkout without symlink support
RUN rm -rf ice-frost-ext/lf-packages && ln -s ../ice-frost/lf-packages ice-frost-ext/lf-packages

WORKDIR /app/ice-frost-ext
RUN lfc src/GadgetMain.lf
# waits for ICE Teach (Unity) on Kafka, then runs the line in real time (20 min, the GadgetMain timeout)
CMD ["sh", "-c", "python script/wait_unity.py && exec python src-gen/GadgetMain/GadgetMain.py --fast false"]
