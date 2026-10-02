# Table Statements

SQL Data Definition Language (DDL) statements create and manage reference tables.

## CREATE TABLE

The `CREATE TABLE` statement registers a table backed by durable storage that can be joined with streams:

```sql
CREATE TABLE table_name
    ( column_name <data_type> [ ,...n ] )
    WITH ( property_name = expression [, ...] );
```

For detailed table configurations, refer to the [Table Overview Guide](../guide/tables/overview.md).

## DESCRIBE TABLE

The `DESCRIBE TABLE` statement returns the schema and configuration of a registered table:

```sql
DESCRIBE TABLE table_name;
```

## DROP TABLE

The `DROP TABLE` statement removes a registered table definition:

```sql
DROP TABLE table_name;
```

> [!CAUTION]
> Delete all rules that reference the table before running `DROP TABLE`.

## SHOW TABLES

The `SHOW TABLES` statement lists all tables currently registered in the database:

```sql
SHOW TABLES;
```
