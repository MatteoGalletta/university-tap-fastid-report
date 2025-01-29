FROM apache/spark:3.4.3

USER 0

RUN apt update && apt install build-essential libpq-dev -y

RUN pip install --upgrade pip
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY src/main.py /opt/spark/main.py

#USER spark