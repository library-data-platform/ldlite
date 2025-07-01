# Test Setup

Sqlite and Duckdb tests "just work".
Postgres tests will be skipped unless you set them up first.

The postgres tests expect a database with user/password ldlite/ldlite.
The default port of 5432 is also assumed.
You can create one for test purposes using:
```sh
docker run \
    --name ldlite-pg \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD=ldlite \
    -e POSTGRES_USER=ldlite \
    -d postgres:latest

docker inspect ldlite-pg
```
In the docker inspect output you'll be able to find the ip address of the container.

You have to pass the postgres host to the tests using the --pg-host parameter, for example:
```sh
python -m pytest --pg-host 172.17.0.3
# or
pdm run test --pg-host 172.17.0.3
```

Each test creates a fresh and randomly named database to prevent interference.
You may periodically want to clean out the test databases created.
The easiest way is to delete and recreate your docker container,
note the IP of the new container might be different.
