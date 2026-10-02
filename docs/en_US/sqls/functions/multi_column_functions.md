# Multiple Column Functions

> [!NOTE]
> **Verification Status**: Tested and Verified against `rekuiper` engine with live telemetry stream load on **2026-10-01 17:57:05 UTC**.  
> **Scorecard**: **1 Verified, 0 Unsupported, 0 Broken**:  
> - **Verified (1)**: `changed_cols`.

A multiple-column function returns multiple columns in a single invocation. In contrast, standard scalar functions return a single column value for each input row.

You can use multiple-column functions only in the `SELECT` clause of a query.

## CHANGED_COLS

```text
changed_cols(prefix, ignoreNull, colA, colB)
```

Returns columns whose values changed since the previous execution, with names prepended by `prefix`. Refer to [changed_cols](./analytic_functions.md#changed_cols) for complete reference details and examples.
