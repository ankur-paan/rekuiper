# User-Defined Functions (UDF) Management API

The rekuiper REST API manages standalone JavaScript user-defined functions (UDFs). You can create, list, describe, update, and delete functions.

## Create a UDF

Use this endpoint to register a new JavaScript function:

```http
POST http://localhost:9081/udf/javascript
```

Request payload fields:
- `id`: The unique identifier for the function. The script code must define a function with this matching name.
- `description`: An explanation of the function.
- `script`: The JavaScript implementation code.
- `isAgg`: A boolean indicating whether the function operates as an aggregate function.

Example request payload:

```json
{
   "id": "area",
   "description": "calculate area",
   "script": "function area(x, y) { return x * y; }",
   "isAgg": false
}
```

## List UDFs

Use this endpoint to display all JavaScript functions registered on the server:

```http
GET http://localhost:9081/udf/javascript
```

Response sample:

```json
["area"]
```

## Describe a UDF

Use this endpoint to display the definition of a specific function:

```http
GET http://localhost:9081/udf/javascript/{id}
```

Response sample:

```json
{
   "id": "area",
   "description": "calculate area",
   "script": "function area(x, y) { return x * y; }",
   "isAgg": false
}
```

## Delete a UDF

Use this endpoint to delete a JavaScript function:

```http
DELETE http://localhost:9081/udf/javascript/{id}
```

Stop or delete rules that reference the function before deleting it. Running rules continue to use cached function instances until stopped or restarted.

## Update a UDF

Use this endpoint to update an existing JavaScript function:

```http
PUT http://localhost:9081/udf/javascript/{id}
```

If the function does not exist, the server creates it. If it exists, the server replaces it. You must restart running rules to load the updated function script.
