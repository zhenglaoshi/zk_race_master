#!/bin/bash

COMPOSE_VERSION=$(docker-compose --version)
DOCKER_VERSION=$(docker --version)

# Start the docker compose file
echo "Running docker compose up. Docker version $DOCKER_VERSION. Compose version $COMPOSE_VERSION. "

docker-compose up -d