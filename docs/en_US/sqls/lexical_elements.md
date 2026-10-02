# Lexical Elements

rekuiper SQL queries consist of comments, tokens, operators, and literals.

## Comments

Comments document query statements. Block comments start with `/*` and end with `*/`.

## Tokens

rekuiper SQL decomposes input text into four token categories:

- **Identifiers**
- **Keywords**
- **Operators**
- **Literals**

Whitespace characters (spaces, horizontal tabs, carriage returns, and newlines) separate adjacent tokens and are ignored during parsing. The lexical analyzer selects the longest matching sequence of characters to construct each token.

## Identifiers

Identifiers name database entities such as streams, tables, and columns. A standard identifier begins with a letter followed by letters or digits.

Enclose reserved keywords, special characters (such as hyphens and spaces), or multilingual Unicode strings in backticks (`` ` ``):

```sql
SELECT `select`, `and` FROM demo;
SELECT `a-b`, `hello world`, `中文Chinese` FROM demo;
```

## Keywords

The following reserved keywords require backticks when used as entity names in SQL statements:

```text
SELECT, FROM, JOIN, LEFT, INNER, ON, WHERE, GROUP, ORDER, HAVING, BY, ASC, DESC, AND, OR, CASE, WHEN, UNTIL, THEN, ELSE, END, IN, NOT, BETWEEN, LIKE, OVER, PARTITION
```

Example querying a field named `from`:

```sql
SELECT * FROM demo1 WHERE `from` = "device1"
```

Example defining a stream named `stream` with Unicode columns:

```sql
CREATE STREAM `stream` (
    USERID BIGINT,
    FIRST_NAME STRING,
    LAST_NAME STRING,
    NICKNAMES ARRAY(STRING),
    Gender BOOLEAN,
    `地址` STRUCT(STREET_NAME STRING, NUMBER BIGINT)
) WITH (DATASOURCE = "users", FORMAT = "JSON");
```

## Operators

rekuiper supports the following arithmetic, logical, comparison, and access operators:

```text
+, -, *, /, %, &, |, ^, =, !=, <, <=, >, >=, [], ->, (), IN, NOT IN, BETWEEN, NOT BETWEEN
```

## Literals

### Boolean Literals

```text
TRUE, FALSE
```

Example query:

```sql
SELECT TRUE AS field1 FROM demo
```

The output field `field1` always returns `true`.

### Time Unit Literals

The following literals specify time durations in window specifications:

- `DD`: Days
- `HH`: Hours
- `MI`: Minutes
- `SS`: Seconds
- `MS`: Milliseconds

### String Literals

Enclose string literals in single quotes or double quotes:

```text
"user", 'user'
```

When passing rules through shell command lines, nested single quotes can cause premature argument termination:

```shell
bin/kuiper create rule myrule '{"sql": "SELECT lower('abc') FROM demo"}'
```

In this command, the shell evaluates `'abc'` as a variable reference. Use escaped double quotes within single-quoted command payloads to preserve string literals:

```shell
bin/kuiper create rule myrule '{"sql": "SELECT lower(\"abc\") FROM demo"}'
```
