Migration
=====

This component provides the ability to move from various other storage format to a Mindmaps graph.

OWL Migration
----

See `owl_mm_notes.txt` in the owl package to read about the limitations of the OWL migrator.

To migrate an OWL schema:
```
mvn exec:java -Dexec.mainClass="io.mindmaps.migration.owl.Main" -Dexec.args="-owl /path/to/owl/file"
```

Optionally you can provide:
```
-graph <graph name>
-engine <Mindmaps engine URL>"
```

CSV migration
----

To migrate a CSV schema and data:
```
mvn exec:java -Dexec.mainClass="io.mindmaps.migration.csv.Main" -Dexec.args="-file /path/to/csv/file -graph icij" -Dmindmaps.conf="../../conf/main/mindmaps-engine.properties"
```

Optionally you can provide:
```
-engine <Mindmaps engine URL>
-as <name of this entity type>"
```


SQL Migration
-----

To migrate an SQL database and data:
```
mvn exec:java -Dexec.mainClass="io.mindmaps.migration.sql.Main" -Dexec.args="-driver your.jdbc.Driver -user jdbcUsername -pass jdbcPassword -database databaseUrl -graph graphName" -Dmindmaps.conf="../../conf/main/mindmaps-engine.properties"
```

Optionally you can provide:
```
-engine <Mindmaps engine URL>
```