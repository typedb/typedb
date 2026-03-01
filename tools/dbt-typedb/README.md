# dbt-typedb

A thin [dbt](https://www.getdbt.com/) adapter for TypeDB's Postgres-compatible wire protocol.

TypeDB exposes materialized projections through a pgwire endpoint that speaks
the Postgres v3 wire protocol. Because dbt already has first-class Postgres
support, this adapter simply re-uses `dbt-postgres` with preconfigured defaults.

## Quick start

```bash
pip install dbt-postgres
```

Create a `profiles.yml`:

```yaml
typedb:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: typedb
      pass: ""
      dbname: typedb
      schema: public
      threads: 1
```

Then run:

```bash
dbt debug          # verify connection
dbt run            # run models (SELECT-only; TypeDB projections are read-only)
dbt docs generate  # introspect schema
```

## Limitations

- **Read-only**: TypeDB projections are materialized views. dbt models that
  generate `CREATE TABLE` / `INSERT` statements will fail. Use `ephemeral`
  materializations or custom macros that wrap `SELECT` queries.
- **No transactions**: `BEGIN` / `COMMIT` / `ROLLBACK` are accepted but ignored.
- **Minimal catalog**: `information_schema` and `pg_catalog` expose projections
  only; system tables from a real Postgres instance are not present.

## Profile template

```yaml
# ~/.dbt/profiles.yml
typedb:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('TYPEDB_HOST', 'localhost') }}"
      port: "{{ env_var('TYPEDB_PORT', '5432') | int }}"
      user: "{{ env_var('TYPEDB_USER', 'typedb') }}"
      pass: "{{ env_var('TYPEDB_PASS', '') }}"
      dbname: typedb
      schema: public
      threads: 4
```
