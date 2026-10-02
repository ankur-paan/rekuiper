# Tables Management

The rekuiper table command-line interface manages table definitions. You can create, describe, list, and drop tables.

## Create a Table

Use this command to create a new table. For table syntax details, refer to [Tables](../../sqls/tables.md).

```shell
create table $table_name $table_def | create table -f $table_def_file
```

### Specify the Table Definition on the Command Line

Example command:

```shell
# bin/kuiper create table my_table '(id bigint, name string, score float) WITH ( datasource = "lookup.json", FORMAT = "json", KEY = "id");'
table my_table created
```

This command creates a table named `my_table`.

### Specify the Table Definition in a File

Use the `-f` flag to load table definitions from a text file:

```shell
# bin/kuiper create table -f /tmp/my_table.txt
table my_table created
```

The file `/tmp/my_table.txt` contains:

```sql
my_table(id bigint, name string, score float)
    WITH ( datasource = "lookup.json", FORMAT = "json", KEY = "id");
```

## Show Tables

Use this command to display all tables defined in the server:

```shell
show tables
```

Example command:

```shell
# bin/kuiper show tables
my_table
```

## Describe a Table

Use this command to display the schema and configuration options of a table:

```shell
describe table $table_name
```

Example output:

```shell
# bin/kuiper describe table my_table
Fields
--------------------------------------------------------------------------------
id  bigint
name  string
score  float

FORMAT: json
KEY: id
DATASOURCE: lookup.json
```

> [!NOTE]
> The CLI does not support direct interactive queries against tables. To inspect table data, join the table with an input stream in a rule query.

## Drop a Table

Use this command to delete a table definition:

```shell
drop table $table_name
```

Example command:

```shell
# bin/kuiper drop table my_table
table my_table dropped
```
