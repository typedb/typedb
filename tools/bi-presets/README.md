# TypeDB BI Tool Connection Presets

Connection configuration for popular BI tools. TypeDB's pgwire endpoint
is compatible with any tool that speaks the PostgreSQL wire protocol.

## Common settings

| Setting    | Value         |
|------------|---------------|
| Driver     | PostgreSQL    |
| Host       | `localhost`   |
| Port       | `5432`        |
| Database   | `typedb`      |
| Username   | `typedb`      |
| Password   | *(empty)*     |
| SSL        | Off (default) |

---

## Looker

**Admin → Database → Connections → Add Connection**

| Field              | Value                |
|--------------------|----------------------|
| Name               | `typedb`             |
| Dialect            | PostgreSQL 9.5+      |
| Host               | `localhost`           |
| Port               | `5432`               |
| Database           | `typedb`             |
| Username           | `typedb`             |
| Password           | *(empty)*            |
| Schema             | `public`             |
| Additional JDBC    | *(leave blank)*      |
| Max connections    | `5`                  |

### LookML project settings

```lookml
connection: "typedb"

explore: my_projection {
  sql_table_name: public.my_projection ;;
}
```

---

## Power BI

**Get Data → PostgreSQL database**

| Field     | Value                 |
|-----------|-----------------------|
| Server    | `localhost:5432`      |
| Database  | `typedb`              |
| Data mode | Import (recommended)  |

Then enter `typedb` / *(empty)* for credentials.

### Notes

- Use **Import** mode — DirectQuery requires full SQL push-down which
  TypeDB projections don't support.
- Power BI's navigator will show tables from `information_schema.tables`
  and column metadata from `information_schema.columns`.

---

## Metabase

**Admin → Databases → Add database**

| Field               | Value         |
|---------------------|---------------|
| Database type       | PostgreSQL    |
| Display name        | `TypeDB`      |
| Host                | `localhost`   |
| Port                | `5432`        |
| Database name       | `typedb`      |
| Username            | `typedb`      |
| Password            | *(empty)*     |

### Sync

Metabase will automatically sync schema metadata on connection.
The `information_schema` and `pg_catalog` virtual tables provide
enough metadata for Metabase to discover tables and columns.

---

## DBeaver / pgAdmin / DataGrip

Any standard PostgreSQL client works out of the box:

```
Host:     localhost
Port:     5432
Database: typedb
User:     typedb
Password: (empty)
```

Or via connection string:

```
postgresql://typedb@localhost:5432/typedb
```

---

## psql (built-in)

```bash
typedb psql
# or with custom host/port:
typedb psql --host myserver --port 5432
```

Or directly:

```bash
psql -h localhost -p 5432 -U typedb -d typedb
```
