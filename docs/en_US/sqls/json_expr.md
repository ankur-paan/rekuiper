# JSON Expressions

rekuiper provides dot, arrow, indexing, and slicing operators to access nested attributes in JSON objects and arrays.

The following sample record illustrates expression syntax:

```json
{
  "name": {"first": "Tom", "last": "Anderson"},
  "age": 37,
  "children": ["Sara", "Alex", "Jack"],
  "fav.movie": "Deer Hunter",
  "friends": [
    {"first": "Dale", "last": "Murphy", "age": 44},
    {"first": "Roger", "last": "Craig", "age": 68},
    {"first": "Jane", "last": "Murphy", "age": 47}
  ],
  "followers": {
    "Group1": [
      {"first": "John", "last": "Shavor", "age": 22},
      {"first": "Ken", "last": "Miller", "age": 33}
    ],
    "Group2": [
      {"first": "Alice", "last": "Murphy", "age": 33},
      {"first": "Brian", "last": "Craig", "age": 44}
    ]
  },
  "ops": {
    "functionA": {"numArgs": 2},
    "functionB": {"numArgs": 3},
    "functionC": {"variadic": true}
  },
  "x": 0,
  "y": 2
}
```

## Basic Expressions

### Property Access

Use the dot (`.`) or arrow (`->`) operator to access nested attributes in a struct or JSON object:

```sql
SELECT demo.age FROM demo;
-- Output: {"age": 37}

SELECT demo.name->first FROM demo;
-- Output: {"first": "Tom"}

SELECT demo.name.first FROM demo;
-- Output: {"first": "Tom"}

SELECT name.first AS fname FROM demo;
-- Output: {"fname": "Tom"}

SELECT name->first AS fname FROM demo;
-- Output: {"fname": "Tom"}

SELECT ops->functionA.numArgs AS num FROM demo;
-- Output: {"num": 2}
```

### Index Expressions

Use bracket notation (`[index]`) to retrieve specific elements from an array. Indices are zero-based. Negative indices count backward from the end of the array (`-1` represents the last item):

```sql
SELECT children FROM demo;
-- Output: {"children": ["Sara", "Alex", "Jack"]}

SELECT children[0] FROM demo;
-- Output: {"children": "Sara"}

SELECT children[1] FROM demo;
-- Output: {"children": "Alex"}

SELECT children[-1] FROM demo;
-- Output: {"children": "Jack"}

SELECT children[-2] FROM demo;
-- Output: {"children": "Alex"}

SELECT d.friends[0]->last FROM demo AS d;
-- Output: {"last": "Murphy"}
```

### Slicing Expressions

Use slice notation (`[start:end]`) to extract contiguous elements from an array. The interval includes `start` and excludes `end` (`[start, end)`):

- If `start` is omitted, slicing begins at the first element.
- If `end` is omitted, slicing continues to the end of the array.

```sql
SELECT children[0:1] FROM demo;
-- Output: {"children": ["Sara"]}

SELECT children[1:-1] FROM demo;
-- Output: {"children": ["Alex"]}

SELECT children[0:-1] FROM demo;
-- Output: {"children": ["Sara", "Alex"]}

SELECT children[:] FROM demo;
-- Output: {"children": ["Sara", "Alex", "Jack"]}

SELECT children[:2] FROM demo;
-- Output: {"children": ["Sara", "Alex"]}

SELECT children[x:y] FROM demo;
-- Output: {"children": ["Sara", "Alex", "Jack"]}

SELECT children[x+1:y] FROM demo;
-- Output: {"children": ["Alex", "Jack"]}

SELECT followers->Group1[:1]->first FROM demo;
-- Output: {"first": ["John"]}
```

## JSONPath Functions

rekuiper provides built-in functions to query complex struct and array columns by using JSONPath expressions:

- `json_path_exists(col, jsonpath)`: Returns `true` if the path matches content.
- `json_path_query(col, jsonpath)`: Returns an array of matched values.
- `json_path_query_first(col, jsonpath)`: Returns the first matching element.

Refer to the [JSON Functions Reference](./functions/json_functions.md) for detailed descriptions.

### JSONPath Syntax

- `.` navigates down the object hierarchy.
- `[]` accesses array items or map fields.
- `$` represents the root document.
- `@` represents the current evaluation node.

Using the sample data:

- `$.age` resolves to `37`.
- `$.friends.first` resolves to `"Dale"`.
- `$.friends` resolves to the entire array.
- `$.friends[0]` selects the first friend in the list.
- `$.friends[0]['last']` selects the `last` attribute of the first friend.
- `$.friends[? @.age > 60].first` selects first names where age exceeds 60.

> [!NOTE]
> Include a space after the `?` character in JSONPath filter expressions.

### Example Queries

Extract the last names of all followers in `Group1`:

```sql
SELECT json_path_query(followers, "$.Group1[*].last") FROM demo;
-- Output: ["Shavor", "Miller"]
```

Filter records where any follower in `Group1` is older than 30:

```sql
SELECT name->last FROM demo WHERE json_path_exists(followers, "$.Group1[? @.age > 30]");
-- Output: {"last": "Anderson"}
```

Access attributes containing special characters or periods:

```sql
SELECT json_path_exists(followers, "$[\"my.follower\"]") FROM demo;
```
