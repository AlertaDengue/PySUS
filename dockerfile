FROM jupyter/scipy-notebook

LABEL maintainer="Flávio Codeço Coelho<fccoelho@gmail.com>"

USER root

RUN apt-get -qq update --yes \
  && apt-get -qq install --yes --no-install-recommends \
  build-essential \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Copy environment file to tmp/
COPY environment.yaml /tmp/environment.yaml

# Use environment to update the env base
RUN conda update -n base -c conda-forge conda \
  && conda env update --file /tmp/environment.yaml --name base \
  && conda clean -afy

USER $NB_UID

RUN pip install pysus
