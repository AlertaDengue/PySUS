FROM condaforge/mambaforge

LABEL maintainer="es.loch@gmail.com"

USER root

ENV DEBIAN_FRONTEND=noninteractive

ENV HOME "/home/pysus"
ENV PATH "$PATH:/home/pysus/.local/bin"
ENV ENV_NAME pysus
ENV PATH "/opt/conda/envs/$ENV_NAME/bin:$PATH"
ENV PATH "/opt/poetry/bin:$PATH"

RUN apt-get -qq update --yes \
  && apt-get -qq install --yes --no-install-recommends \
  build-essential \
  firefox \
  ca-certificates \
  sudo \
  curl \
  && rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash pysus \
  && echo "pysus ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/pysus \
  && chmod 0440 /etc/sudoers.d/ \
  && echo 'source /opt/conda/bin/activate "$ENV_NAME" && exec "$@"' > /activate.sh \
  && echo 'source activate "$ENV_NAME"' >  /home/pysus/.bashrc \
  && chmod +x /activate.sh \
  && chmod -R a+rwx /opt/conda /tmp \
  && sudo chown -R pysus:pysus /usr/src

COPY --chown=pysus:pysus conda/dev.yaml /tmp/dev.yaml
COPY --chown=pysus:pysus docker/scripts/entrypoint.sh /entrypoint.sh
COPY --chown=pysus:pysus docker/scripts/poetry-install.sh /tmp/poetry-install.sh
COPY --chown=pysus:pysus pyproject.toml poetry.lock LICENSE README.md /usr/src/
COPY --chown=pysus:pysus pysus /usr/src/pysus
COPY --chown=pysus:pysus docs/source/**/*.ipynb /home/pysus/Notebooks/
COPY --chown=pysus:pysus docs/source/data /home/pysus/Notebooks/

USER pysus

RUN mamba env create -n $ENV_NAME --file /tmp/dev.yaml \
  && mamba clean -afy

RUN cd /usr/src/ && bash /tmp/poetry-install.sh

WORKDIR /home/pysus/Notebooks

ENTRYPOINT ["bash", "/activate.sh", "jupyter", "notebook", "--port=8888", "--ip=0.0.0.0"]
