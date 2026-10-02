# Object Functions

> [!NOTE]
> **Verification Status**: Tested and Verified against `rekuiper` engine with live telemetry stream load on **2026-10-01 17:54:00 UTC**.  
> **Scorecard**: **10 Verified, 0 Unsupported, 0 Broken**:  
> - **Verified (10)**: `keys`, `values`, `object`, `zip`, `items`, `object_construct`, `object_concat`, `erase`, `object_pick`, `obj_to_kvpair_array`.

Object functions construct, manipulate, and extract data from JSON objects and map structures.

## KEYS

```text
keys(obj)
```

Returns an array that contains all keys of the object.

Example:

```sql
keys({"a":1, "b":2})
```

Result:

```sql
["a","b"]
```

## VALUES

```text
values(obj)
```

Returns an array that contains all values of the object.

Example:

```sql
values({"a":1, "b":2})
```

Result:

```sql
[1,2]
```

## OBJECT

```text
object(keys, values)
```

Constructs an object from an array of string keys and an array of corresponding values. Both arrays must have the same length.

Example:

```sql
object(["a","b"],[1,2])
```

Result:

```sql
{"a":1, "b":2}
```

## ZIP

```text
zip(entries)
```

Constructs an object from an array of two-element entry arrays. In each entry array, the first element is a string key and the second element is the value.

Example:

```sql
zip([["a",1],["b",2]])
```

Result:

```sql
{"a":1, "b":2}
```

## ITEMS

```text
items(obj)
```

Returns an array of key-value pairs from the object. Each item in the returned array is a two-element array where the first element is the key and the second element is the value.

Example:

```sql
items({"a":1, "b":2})
```

Result:

```sql
[["a",1],["b",2]]
```

## OBJECT_CONSTRUCT

```text
object_construct(key1, col1, key2, col2, ...)
```

Returns an object constructed from key-value argument pairs. The function requires an even number of arguments. Keys must be strings. If a value is null, the function omits that key-value pair from the output object.

Example:

```sql
object_construct("a", 1, "b", 2)
```

Result:

```sql
{"a":1, "b":2}
```

## OBJECT_CONCAT

```text
object_concat(obj1, obj2, ...)
```

Merges multiple input objects into a single new object. The function requires at least two input objects. When duplicate keys exist across objects, the value from the last object in the argument list overwrites earlier values.

Example:

```sql
object_concat({"a": 1}, {"b": 2}, {"b": 3})
```

Result:

```sql
{"a":1, "b":3}
```

## ERASE

```text
erase(obj, k)
```

Removes specified keys from an object. If `k` is a string, the function removes that single key. If `k` is an array of strings, the function removes all matching keys.

Example:

```sql
erase({"baz": [1, 2, 3], "bar": 'hello world',"foo":'emq'}, 'foo')
```

Result:

```sql
{"baz": [1, 2, 3], "bar": 'hello world'}
```

## OBJECT_PICK

```text
object_pick(obj, k)
```

Extracts only the specified keys from an object. If `k` is a string, the function retains only that key. If `k` is an array of strings, the function retains all matching keys.

Example:

```sql
object_pick({"baz": [1, 2, 3], "bar": 'hello world',"foo":'emq'}, 'foo')
```

Result:

```sql
{"foo":'emq'}
```

## OBJ_TO_KVPAIR_ARRAY

```text
obj_to_kvpair_array(obj)
```

Converts an object into an array of key-value objects.

Example:

```sql
obj_to_kvpair_array({"key1":1, "key2":2})
```

Result:

```sql
[{"key":"key1", "value":1},{"key":"key2", "value":2}]
```
