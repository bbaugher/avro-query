Avro Query
==========

Query API/CLI over Avro data using Crunch. The query format is Splunk-like to 
provide simplicity.

Getting Started
---------------

With the deploy-able artifact (`mvn clean install`),

```
tar zxf avro-query-1.0-SNAPSHOT-assembly.tar.gz
./avro-query-1.0-SNAPSHOT/bin/avro-query query PATH_TO_AVRO_FILES "QUERY"
```

The `PATH_TO_AVRO_FILES` can either by an avro file or directory with avro 
files.

The `QUERY` can be of the form,

 * `myField >= 3 AND anotherField = value OR NOT myField < 1`
 * Blank indicates read all data
 * `NOT myField = null | stats sum(myField) by anotherField`
   * Search for all data where `myField` is not `null` and then provide the sum of `myField` and group by `anotherField`

Query Details
-------------

Currently a query is of the form `select query | stats query`. Where the select 
query filters what data you want to see from the files provided. Optionally you can 
specify a stats query which can do basic math functions like `count`, `sum`, `min` 
and `max`. 

The select query is optional to where a blank query is valid (read all data) and 
the following query ` | stats count` (count all data) is also valid.

### Select Query

The select query is effectively just repeating this pattern, `FIELD_NAME OPERATOR VALUE`,
where `FIELD_NAME` is the name of an avro field in your data, `OPERATOR` is any 
logical operator `>`, `>=`, `<`, `<=` and `=`, and `VALUE` is any value the avro 
data may contain. The select query then joins zero to many of these statements 
together with boolean operators `AND` and `OR`. Additionally you can use `NOT` 
to specify inverse (i.e. `NOT myField = 3`). If a boolean operator is not 
specified between parts `AND` is assumed.

### Stats Query

The stats query is of the form `stats FUNCTION(FIELD*) by FIELD1, FIELD2, ...`.
The possible `FUNCTION`s include `count`, `min`, `max` and `sum`. `count` does 
not require you specify a field for its function while all the others do. 
The grouping by fields are optional and any number of fields may be specified.

In order to write a stats query you must use the `|` character after your select 
query (i.e. `myField > 3 | stats count by myField`).

CLI Docs
--------

Currently the only command is the `query` command. Run `help query` for information 
about the command.

### Environment Variables

 * `$JAVA_HOME` : The location of the java installation
 * `$AVRO_QUERY_LOG4J_OPTIONS` : The log4j options. Defaults to `-Dlog4j.configuration=file:$base_dir/config/log4j.properties`
 * `$AVRO_QUERY_LIB_DIR` : The location of the avro query lib directory. Defaults to `$base_dir/lib`.
 * `$AVRO_QUERY_JAVA_OPTIONS` : The java options for the avro query java command. Defaults to no options
 * `$AVRO_QUERY_JAVA_LIB_OPTIONS` : The java option for the java library. Defaults `-Djava.library.path=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native` if the cloudera installation can be found
 * `$CLASSPATH` : The classpath used for the java command. Includes hadoop's classpath if installed and the avro query lib directory
 
Technology
----------

 * [Apache Crunch](https://crunch.apache.org/) for calculating the result
 * [Antlr](http://www.antlr.org/) for parsing query text
 * [Airline](https://github.com/airlift/airline) for CLI
 * [Avro](https://avro.apache.org/) for reading data
 
Future Ideas
------------

 * Support sort command
 * Support timechart command by specifying a date field
 * Support more stats functions (avg, first, last)
 * Support multiple stats functions per query (i.e. `stats min(x), max(x)`)