FROM jupyter/scipy-notebook

LABEL maintainer="Flávio Codeço Coelho<fccoelho@gmail.com>"

USER root

RUN apt update && \
    apt install -y libffi-dev && \
    rm -rf /var/lib/apt/lists/*

USER $NB_UID

RUN pip install -U \
    pandas \
    cffi \
    dbfread \
    geocoder \
    requests \
    folium \
    fastparquet \
    geopandas \
    pysus

