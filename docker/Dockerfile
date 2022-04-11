FROM condaforge/mambaforge

LABEL maintainer="es.loch@gmail.com"

USER root

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get -qq update --yes \
  && apt-get -qq install --yes --no-install-recommends \
  build-essential firefox\
  ca-certificates sudo \
  && rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash developer \
  && echo "developer ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/developer \
  && chmod 0440 /etc/sudoers.d/

# Set home to developer
ENV HOME /home/developer
# Allow the application to execute as any UID 
# and still write temporary data to /tmp
ENV APP_TMP_DATA=/tmp

# Copy environment file to tmp/
COPY environment.yaml ${APP_TMP_DATA}/environment.yaml
# Executable in a container to be owned by the root user
COPY docker/test_notebooks.sh /test_notebooks.sh
COPY docker/entrypoint.sh /entrypoint.sh

# Use environment to update the env base
RUN mamba update --all --yes --quiet \
  && mamba env update --file ${APP_TMP_DATA}/environment.yaml --name base \
  && mamba clean -afy

USER developer

# Copy only notebooks to the container with permissions to user
RUN mkdir ${HOME}/Notebooks
WORKDIR ${HOME}/Notebooks
COPY pysus/Notebooks .

ENTRYPOINT ["/entrypoint.sh"]

CMD ["/usr/bin/firefox"]
