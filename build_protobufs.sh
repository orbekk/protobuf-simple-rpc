#!/bin/bash

find src/test/java -name '*.proto' -exec \
    protoc {} --java_out=src/test/java \;

find src/main/java -name '*.proto' -exec \
    protoc {} --java_out=src/main/java \;
