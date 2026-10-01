# Multiple Column Functions

> [!NOTE]
> **Verification Status**: Tested and Verified against `rekuiper` engine with live telemetry stream load on **2026-10-01 17:57:05 UTC**.  
> **Scorecard**: **1 Verified, 0 Unsupported, 0 Broken**:  
> - **Verified (1)**: `changed_cols`.

A multiple column function is a function that returns multiple columns. Contrast to normal scalar function, which
returns a single column of a single row.

Multiple column function can only be used in the `SELECT` clause of a query.

## CHANGED_COLS

```text
changed_cols(prefix, ignoreNull, colA, colB)
```

Return the changed columns whose name is prefixed. Check [changed_cols](./analytic_functions.md#changedcols-function)
for detail.
