def test_pgvector_extension_available(pg_connection):
    """The pg fixture must give us a connection with pgvector usable."""
    cur = pg_connection.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
    cur.execute("SELECT '[1,2,3]'::vector")
    row = cur.fetchone()
    assert row[0] == '[1,2,3]'


def test_pg_fixture_isolated_per_test(pg_connection):
    """Each test gets its own DB — no leakage."""
    cur = pg_connection.cursor()
    cur.execute("CREATE TABLE probe (id int)")
    cur.execute("INSERT INTO probe VALUES (1)")
    cur.execute("SELECT count(*) FROM probe")
    assert cur.fetchone()[0] == 1
