ARG BASE=debian:buster
FROM $BASE
 
LABEL maintainer="Nexus Developers (dkv-dev@googlegroups.com)"

# Install basic utilities
RUN apt-get update && apt-get install --yes --allow-unauthenticated adduser vim sudo git curl unzip build-essential \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
 
# Install GoLang
RUN curl -fsSL https://dl.google.com/go/go1.18.2.linux-amd64.tar.gz | tar xz \
    && chown -R root:root ./go && mv ./go /usr/local
ENV PATH="/usr/local/go/bin:${PATH}"

# Install Protobuf
RUN curl -fsSL -O https://github.com/protocolbuffers/protobuf/releases/download/v3.15.8/protoc-3.15.8-linux-x86_64.zip \
    && unzip protoc-3.15.8-linux-x86_64.zip -d protoc && chown -R root:root ./protoc && mv ./protoc /usr/local \
    && rm protoc-3.15.8-linux-x86_64.zip
ENV PATH="/usr/local/protoc/bin:${PATH}"

# Install Nexus
ARG CI
RUN if [ -z "$CI" ] ; then  git clone --recursive https://github.com/flipkart-incubator/nexus.git \
    && cd nexus && GOOS=linux GOARCH=amd64 make build \
    && mv ./bin /usr/local/nexus && chown -R root:root /usr/local/nexus; fi
    
ENV PATH="/usr/local/nexus:${PATH}"
ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"

