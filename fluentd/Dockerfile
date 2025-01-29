FROM fluent/fluentd:v1.17.1-debian-1.1

USER root

RUN apt-get update && apt -y install build-essential default-libmysqlclient-dev libpq-dev
RUN gem install fluent-plugin-kafka fluent-plugin-sql mysql2 pg --no-document

CMD ["-c", "/fluentd/etc/fluentd.conf"]

COPY fluentd.conf /fluentd/etc/fluentd.conf