# Vector Functions

This document describes the vector functions in `rekuiper`.

Vector functions calculate vector similarity and perform nearest-neighbor searches. You can use these functions in SQL queries on edge nodes.

## Function Overview

| Function | Description |
| :--- | :--- |
| `cosine_similarity(vec1, vec2)` | Returns the cosine similarity between two numeric vectors. |
| `vector_l2(vec1, vec2)` | Returns the Euclidean distance ($L_2$ norm) between two numeric vectors. |
| `vector_dot(vec1, vec2)` | Returns the inner dot product of two numeric vectors. |
| `vector_match(query_vec, candidate_array, top_k)` | Returns the top $K$ nearest vectors ordered by cosine similarity. |

---

## COSINE_SIMILARITY

```text
cosine_similarity(vec1, vec2)
```

The `cosine_similarity` function calculates the cosine of the angle between two vectors.

### Arguments

- `vec1`: Array of numbers (integer or float).
- `vec2`: Array of numbers. `vec2` must have the same length as `vec1`.

### Return Value

Returns a float value between `-1.0` and `1.0`.
- `1.0` means the vectors have identical direction.
- `0.0` means the vectors are orthogonal.
- `-1.0` means the vectors have opposite direction.

The function returns `null` if:
- An argument is not an array.
- The two arrays do not have equal length.
- The magnitude of either vector is zero.

### Example

```sql
SELECT 
    device_id,
    cosine_similarity(current_features, [0.1, 0.4, 0.9]) AS similarity
FROM sensor_stream
WHERE cosine_similarity(current_features, [0.1, 0.4, 0.9]) > 0.85;
```

---

## VECTOR_L2

```text
vector_l2(vec1, vec2)
```

The `vector_l2` function calculates the Euclidean distance between two vectors.

### Arguments

- `vec1`: Array of numbers.
- `vec2`: Array of numbers with the same length as `vec1`.

### Return Value

Returns a float value greater than or equal to `0.0`.
- A value of `0.0` means the two points are identical.
- Larger values indicate greater geometric distance.

The function returns `null` if the vector lengths are not equal.

### Example

```sql
SELECT 
    machine_id,
    vector_l2(vibration_spectrum, baseline_spectrum) AS distance
FROM factory_events
WHERE vector_l2(vibration_spectrum, baseline_spectrum) > 3.0;
```

---

## VECTOR_DOT

```text
vector_dot(vec1, vec2)
```

The `vector_dot` function calculates the dot product of two vectors.

### Arguments

- `vec1`: Array of numbers.
- `vec2`: Array of numbers with the same length as `vec1`.

### Return Value

Returns the float sum of the products of corresponding elements. Returns `null` if the array lengths are not equal.

### Example

```sql
SELECT 
    model_id,
    vector_dot(weights, inputs) AS score
FROM inference_events;
```

---

## VECTOR_MATCH

```text
vector_match(query_vec, candidate_array, top_k)
```

The `vector_match` function compares a query vector to an array of candidates. The function returns the top $K$ items with the highest cosine similarity.

### Arguments

- `query_vec`: Array of numbers representing the reference vector.
- `candidate_array`: Array of vectors, or array of objects with a `"vector"` or `"embedding"` field.
- `top_k`: Integer that specifies the maximum number of items to return.

### Return Value

Returns an array of candidate objects. Each object contains a `similarity` property with the calculated cosine similarity score. The items are sorted in descending order of similarity.

### Example

```sql
SELECT 
    query_id,
    vector_match(query_embedding, candidate_documents, 5) AS top_matches
FROM search_events;
```
