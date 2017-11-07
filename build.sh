#!/usr/bin/env bash

# Build
echo "Building"
mvn clean package

# Server
echo "Setting up server"
cd server/target
tar xzf pod-tp-server-1.0-SNAPSHOT-bin.tar.gz

# Client
echo "Setting up client"
cd ../../client/target
tar xzf pod-tp-client-1.0-SNAPSHOT-bin.tar.gz

cd ../../