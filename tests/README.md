# Test Setup

Aurora-backend tests use `pytest-postgresql` in `noproc` mode pointed at a
Docker-managed Postgres with pgvector. **No local Postgres install required.**

## Start the test database

```bash
docker compose -f docker-compose.test.yml up -d test-pg
```

Image: `pgvector/pgvector:pg16` on `localhost:54329` (user/password `test`,
superuser `test`). The `54329` port avoids host-Postgres collisions.

## Run the tests

```bash
pytest tests/test_pg_fixture.py -v
```

## Stop the database

```bash
docker compose -f docker-compose.test.yml down
```

## CI

CI runs the same `docker compose up` step before `pytest`. The container is
recreated per-job so there is no state carried between runs.

## Notes

- `pytest-postgresql` and `psycopg` must be installed in the Python environment
  used to run `pytest`. In this repo they are installed in the system Python
  (not the project venv). Use `python3 -m pytest` rather than the venv's
  `pytest` binary.
- The fixture uses `dbname="pytest_db"` (not `"test"`) to avoid a collision
  with the pre-existing `test` database created by the Docker container's
  `POSTGRES_DB` environment variable. The `DatabaseJanitor` creates
  `pytest_db` and `pytest_db_tmpl` fresh on each test session and drops them
  on teardown.
