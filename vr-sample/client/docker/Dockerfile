FROM openjdk:8u151-jdk

MAINTAINER Gleb Leonov, Gleb.Leonov@jetbrains.com

ADD run-client.sh .
RUN chmod +x run-client.sh

ADD sample-client-1.0-SNAPSHOT-jar-with-dependencies.jar .

# Start the app
CMD ./run-client.sh