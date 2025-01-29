#FROM jupyter/pyspark-notebook:latest
FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3

USER root
RUN apt-get update && apt-get install -y libpq-dev

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt


USER $NB_UID