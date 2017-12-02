The goal of this project is to build a clean, simple implementation of viewstamped replication based on the following paper: http://pmg.csail.mit.edu/papers/vr-revisited.pdf
The project is implemented in Kotlin, modern and rapidly evolving programming language. Recently it became an official language for Android development.
The project uses Maven build system
Docker and docker-compose were used to create executable demo. To run it, one need to have Java, Maven and Docker installed. Do the following:
* run build.sh from project root folder to build sources and Docker containers
* run “docker-compose up” in vr-sample/docker-compose folder to start demo  
To see how views are changing, one can stop and restart containers via docker-compose commands
