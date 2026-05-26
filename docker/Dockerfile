FROM python:3.12-slim

LABEL maintainer="luabidaa@gmail.com"
LABEL org.opencontainers.image.source="https://github.com/AlertaDengue/PySUS"
LABEL org.opencontainers.image.description="PySUS - Data extraction from Brazilian public health systems"

ENV DEBIAN_FRONTEND=noninteractive
ENV HOME "/home/pysus"
ENV PATH "$PATH:/home/pysus/.local/bin"

RUN apt-get -qq update --yes \
  && apt-get -qq install --yes --no-install-recommends \
  libffi-dev \
  build-essential \
  ca-certificates \
  sudo \
  curl \
  && rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash pysus \
  && echo "pysus ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/pysus \
  && chmod 0440 /etc/sudoers.d/pysus \
  && mkdir -p /home/pysus/Notebooks \
  && chown -R pysus:pysus /home/pysus

COPY pyproject.toml poetry.lock LICENSE README.md /usr/src/
COPY pysus /usr/src/pysus
COPY docker/scripts/entrypoint.sh /entrypoint.sh
COPY docker/notebooks/ /home/pysus/Notebooks/

RUN pip install poetry \
  && cd /usr/src && poetry config virtualenvs.create false && poetry install --with docs \
  && pip install 'httpx<0.28' \
  && chown -R pysus:pysus /home/pysus

USER pysus
WORKDIR /home/pysus/Notebooks

ENTRYPOINT ["/entrypoint.sh"]
