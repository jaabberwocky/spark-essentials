#!/bin/bash

docker-compose down
docker-compose up --scale spark-worker=3