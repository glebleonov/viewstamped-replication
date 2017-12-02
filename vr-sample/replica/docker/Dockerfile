FROM openjdk:8u151-jdk

MAINTAINER Gleb Leonov, Gleb.Leonov@jetbrains.com

ADD run-replica.sh .
RUN chmod +x run-replica.sh

ADD sample-replica-1.0-SNAPSHOT-jar-with-dependencies.jar .

# Start the app
CMD ./run-replica.sh