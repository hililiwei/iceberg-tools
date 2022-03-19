# Iceberg Tools
Some gadgets for Iceberg. This project is based on Apache Avro and Apache Iceberg.



```
java -jar iceberg-tools-1.0-SNAPSHOT.jar

Available tools:
manifest2json  Dumps an Iceberg Manifest Avro data file as JSON, record per line or pretty.

```

## manifest2json

Dumps an Iceberg Manifest Avro data file as JSON, Some binary fields, such as `lower_bounds`, will be  converted to the actual type based on the **schema** in the `metadata-json-file`, and then converted to String for display.

```
java -jar iceberg-tools-1.0-SNAPSHOT.jar manifest2json

manifest2json [--pretty] [--head[=X]] manifest-file metadata-json-file

Dumps an Iceberg Manifest Avro data file as JSON, Some binary fields, such as `lower_bounds`, will be  converted to the actual type based on the schema in the  metadata-json-file, and then converted to String for display.

record per line or pretty.

A dash ('-') can be given as an input file to use stdin

Option                         Description
------                         -----------
--head [String]                Converts the first X records (default is 10).
--pretty                       Turns on pretty printing.
--reader-schema [String]       Reader schema
--reader-schema-file [String]  Reader schema file

```
