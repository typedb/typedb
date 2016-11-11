Migration
=====

This component provides the ability to move from various other storage format to a Grakn graph.

OWL Migration
----

See `owl_mm_notes.txt` in the owl package to read about the limitations of the OWL migrator.

To migrate an OWL schema:
```
./migration.sh owl -file /path/to/owl/file"
```

Optionally you can provide:
```
-graph <graph name>
-engine <Grakn engine URL>"
```

CSV migration
----

To migrate a CSV schema and data:
```
./migration.sh csv -file /path/to/csv/file -graph icij
```

Optionally you can provide:
```
-engine <Grakn engine URL>
-as <name of this entity type>"
```


SQL Migration
-----

To migrate an SQL database and data:
```
./migration.sh sql -driver your.jdbc.Driver -user jdbcUsername -pass jdbcPassword -database databaseUrl -graph graphName
```

Optionally you can provide:
```
-engine <Grakn engine URL>
```