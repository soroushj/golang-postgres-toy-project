#!/usr/bin/env bash

# Requirements:
# - docker-compose
# - psql
# - go

DB_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/db" >/dev/null 2>&1 && pwd )"
cd "$DB_DIR"

echo '* Initializing db...'
docker-compose up -d

echo '* Waiting for db to start...'
until psql -U postgres -h localhost -p 5533 -c 'SELECT 1' >/dev/null 2>&1; do
  sleep 1
done

echo '* Running main.go...'
go run ../main.go

echo '* Removing db...'
docker-compose down -v
