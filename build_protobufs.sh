#!/bin/bash

find src/main/java -name '*.proto' -exec \
    protoc {} --java_out=src/main/java \;
