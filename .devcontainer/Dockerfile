FROM mcr.microsoft.com/devcontainers/anaconda:0-3

# Env variables
ENV AWS_PROFILE=default
ENV AWS_DEFAULT_REGION=us-east-1

# Install aws-cli
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install

# Install aws-cdk (with dependency on nodejs)
ENV NVM_DIR /usr/local/nvm
ENV NODE_VERSION v20.10.0
RUN mkdir -p /usr/local/nvm && apt-get update && echo "y" | apt-get install curl
RUN curl -o- https://raw.githubusercontent.com/creationix/nvm/master/install.sh | bash
RUN /bin/bash -c "source $NVM_DIR/nvm.sh && nvm install $NODE_VERSION && nvm use --delete-prefix $NODE_VERSION"
ENV NODE_PATH $NVM_DIR/versions/node/$NODE_VERSION/bin
ENV PATH $NODE_PATH:$PATH
RUN npm install --global aws-cdk

# Init shells for conda
RUN conda init bash && conda init zsh

# # Installs app dependencies from environment.yml in docker build
# COPY environment.yml* .devcontainer/noop.txt /tmp/conda-tmp/
# RUN if [ -f "/tmp/conda-tmp/environment.yml" ]; then umask 0002 && /opt/conda/bin/conda env # update -n base -f /tmp/conda-tmp/environment.yml; fi \
#    && rm -rf /tmp/conda-tmp
