FROM reynoldsm88/centos-jdk:latest

LABEL maintainer="michael.reynolds@twosixlabs.com"

ENV SCALA_VERSION '2.12'
ENV JAVA_OPTS "-Xms512m -Xmx512m -XX:+UseConcMarkSweepGC"
ENV PROGRAM_ARGS "compose"

COPY ./target/scala-$SCALA_VERSION/*assembly*.jar /opt/app/pkg
COPY ./scripts/run.sh /opt/app/bin

RUN chmod -R 755 /opt/app

ENTRYPOINT ["/opt/app/bin/run.sh"]