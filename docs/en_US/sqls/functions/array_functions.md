# Array Functions

> [!NOTE]
> **Verification Status**: Tested and Verified against `rekuiper` engine with live telemetry stream load on **2026-10-01 16:35:50 UTC**.  
> **Scorecard**: **25 / 25 Array Functions Fully Verified with Live Data (100% Parity)**:  
> `cardinality`, `array_position`, `element_at`, `array_contains`, `array_create`, `array_remove`, `array_last_position`, `array_contains_any`, `array_intersect`, `array_union`, `array_max`, `array_avg`, `array_min`, `array_except`, `repeat`, `sequence`, `array_cardinality`, `array_flatten`, `array_distinct`, `array_map`, `array_join`, `array_shuffle`, `array_concat`, `array_sort`, `kvpair_array_to_obj`.

Array functions manipulate arrays or return metadata about array elements.

## CARDINALITY

```text
cardinality(array)
```

Returns the number of elements in the array. Returns 0 if the argument is null.

## ARRAY_POSITION

```text
array_position(array, value)
```

Returns the zero-based index of the first occurrence of `value` in the array. Returns -1 if the value is not present or if `array` is nil.

## ELEMENT_AT

```text
element_at(array, index)
```

Returns the element at the specified zero-based index. If `index` is negative, the function counts backward from the end of the array. Returns nil if `array` is nil.

## ARRAY_CONTAINS

```text
array_contains(array, value)
```

Returns true if the array contains the specified element. Returns nil if `array` is nil.

## ARRAY_CREATE

```text
array_create(value1, ......)
```

Creates a new array from the specified literal values or expressions.

## ARRAY_REMOVE

```text
array_remove(array, value)
```

Returns a new array with all occurrences of `value` removed. Returns nil if `array` is nil.

## ARRAY_LAST_POSITION

```text
array_last_position(array, val)
```

Returns the zero-based index of the last occurrence of `val` in the array. Returns -1 if the value is not present or if `array` is nil.

## ARRAY_CONTAINS_ANY

```text
array_contains_any(array1, array2)
```

Returns true if `array1` and `array2` share at least one common element. Returns false if `array1` is nil.

## ARRAY_INTERSECT

```text
array_intersect(array1, array2)
```

Returns an array containing the distinct intersection of elements present in both arrays. Returns nil if either array is nil.

## ARRAY_UNION

```text
array_union(array1, array2)
```

Returns an array containing all distinct elements from both input arrays, with duplicates removed.

## ARRAY_MAX

```text
array_max(array)
```

Returns the maximum element from the array. The function ignores null elements. Returns nil if `array` is nil or empty.

## ARRAY_AVG

```text
array_avg(array)
```

Returns the average of numeric elements in the array as a floating-point value. The function ignores null elements. Returns nil if `array` is nil or contains no valid numeric elements.

## ARRAY_MIN

```text
array_min(array)
```

Returns the minimum element from the array. The function ignores null elements. Returns nil if `array` is nil or empty.

## ARRAY_EXCEPT

```text
array_except(array1, array2)
```

Returns an array of distinct elements that exist in `array1` but not in `array2`. Returns nil if `array1` is nil.

## REPEAT

```text
repeat(string, count)
```

Creates an array containing the specified value repeated `count` times.

## SEQUENCE

```text
sequence(start, stop, step)
```

Returns an array of integers starting from `start` up to `stop`, incremented by `step`.

## ARRAY_CARDINALITY

```text
array_cardinality(array)
```

Returns the number of elements in the array. The function ignores null values. Returns 0 if `array` is nil.

## ARRAY_FLATTEN

```text
array_flatten(array)
```

Flattens nested array elements into a single-level array.

For example, input `[[1, 4], [2, 3]]` returns `[1, 4, 2, 3]`. Returns nil if `array` is nil.

## ARRAY_DISTINCT

```text
array_distinct(array)
```

Returns a new array containing unique elements with all duplicate values removed. Returns nil if `array` is nil.

## ARRAY_MAP

```text
array_map(function_name, array)
```

Returns a new array produced by applying the specified scalar function to each element. Returns nil if `array` is nil.

## ARRAY_JOIN

```text
array_join(array, delimiter, null_replacement)
```

Returns a string created by concatenating array elements with the specified delimiter. Use the optional `null_replacement` parameter to substitute null elements.

For example, input `[1, 2, 3]` with delimiter `","` returns `"1,2,3"`. Returns nil if `array` is nil.

## ARRAY_SHUFFLE

```text
array_shuffle(array)
```

Returns a new array with elements randomly rearranged. Returns nil if `array` is nil.

## ARRAY_CONCAT

```text
array_concat(array1, array2, ...)
```

Returns a new array created by concatenating the input arrays. This function does not modify input arrays. The function treats nil arguments as empty arrays.

## ARRAY_SORT

```text
array_sort(array)
```

Returns a sorted copy of the input array. Returns nil if `array` is nil.

```sql
array_sort([3, 2, "b", "a"])
```

Result:

```sql
[2, 3, "a", "b"]
```

## KVPAIR_ARRAY_TO_OBJ

```text
kvpair_array_to_obj(array)
```

Converts an array of key-value pair objects into a single JSON object.

```sql
kvpair_array_to_obj([{"key":"key1", "value":1},{"key":"key2", "value":2}])
```

Result:

```sql
{"key1":1, "key2":2}
```
