# dbt-typedb

A thin [dbt](https://www.getdbt.com/) adapter package for TypeDB's pgwire compatibility layer.

TypeDB exposes materialized projections through a read-only Postgres v3 wire
compatibility endpoint. This package registers a real `type: typedb` adapter on
top of `dbt-postgres`, keeps the normal Postgres connection machinery, and
fails fast when dbt tries to execute write-oriented SQL that the compatibility
layer does not support.

## Quick start

```bash
uv venv .venv
# Activate it before installing, for example:
#   Windows PowerShell: .venv\Scripts\Activate.ps1
#   macOS/Linux:        source .venv/bin/activate
uv pip install ./tools/dbt-typedb
```

Create a `profiles.yml`:

```yaml
typedb:
  target: dev
  outputs:
    dev:
      type: typedb
      host: localhost
      port: 5432
      user: admin
      pass: password
      dbname: typedb
      schema: public
      threads: 1
```

Supported workflows:

```bash
dbt debug          # verify connection
dbt parse          # parse the project
dbt compile        # compile SQL
dbt show --inline "select * from public.my_projection limit 5"
dbt test           # run SELECT-based data tests against existing projections
dbt docs generate  # introspect schema
```

Write-oriented commands such as `dbt run`, `dbt build`, `dbt seed`, and
`dbt snapshot` are intentionally blocked.

## What this package ships

- A real `typedb` adapter type that dbt can load after `uv pip install ./tools/dbt-typedb`
- A `dbt init` profile template under `src/dbt/include/typedb/profile_template.yml`
- Read-only SQL guards that stop unsupported DDL and DML before they hit the server
- A dependency on `dbt-postgres`, which provides the shared Postgres connection and catalog behavior

## Limitations

- **Read-only**: TypeDB projections are materialized views. dbt model
  materializations that generate `CREATE`, `INSERT`, `UPDATE`, `DELETE`, or
  `ALTER` statements are rejected.
- **No transactions**: `BEGIN` / `COMMIT` / `ROLLBACK` are accepted but ignored.
- **Minimal catalog**: `information_schema` and `pg_catalog` expose projections
  only; system tables from a real Postgres instance are not present.
- **Compatibility layer**: The goal is client/tool interoperability for
  projection reads, not behavioral parity with PostgreSQL extensions,
  procedures, or server-side write features.

## Profile template

```yaml
# ~/.dbt/profiles.yml
typedb:
  target: dev
  outputs:
    dev:
      type: typedb
      host: "{{ env_var('TYPEDB_HOST', 'localhost') }}"
      port: "{{ env_var('TYPEDB_PORT', '5432') | int }}"
      user: "{{ env_var('TYPEDB_USER', 'admin') }}"
      pass: "{{ env_var('TYPEDB_PASS', 'password') }}"
      dbname: typedb
      schema: public
      threads: 4
```

The ready-to-copy example profile is also available in `profiles.yml` in this
directory.
