#!/usr/bin/env bash

curl -X POST -H "Expect:" -F "jarfile=@/opt/app.jar" http://flink-jobmanager:8081/jars/upload

JAR_ID=$(curl http://flink-jobmanager:8081/jars | jq '.files[0].id' -r)

curl -X POST http://flink-jobmanager:8081/jars/$JAR_ID/run