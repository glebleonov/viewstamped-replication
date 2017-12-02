#!/usr/bin/env bash
cd vr-core
mvn clean install

cd ../vr-sample
mvn clean install

cd replica
mvn docker:build

cd ../client
mvn docker:build