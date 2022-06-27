FROM docker.io/openjdk:15-alpine

ENV DERBY_VERSION 10.15.2.0
ENV DB_DIR /mnt/database

RUN \
mkdir -p $DB_DIR && \
apk update && \
apk add bash && \
apk add ca-certificates && update-ca-certificates && apk add openssl

ADD entrypoint.sh /entrypoint.sh

WORKDIR /opt

# convert line-endings to unix format of entry point
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh

# download and extract derby
RUN wget -O derby.tgz https://dlcdn.apache.org/db/derby/db-derby-${DERBY_VERSION}/db-derby-${DERBY_VERSION}-lib.tar.gz && \
tar -xzf derby.tgz && mv db-derby-${DERBY_VERSION}-lib derby && rm derby.tgz

EXPOSE 1527 

ENTRYPOINT ["/entrypoint.sh"]