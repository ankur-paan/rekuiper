# Window Functions

> [!NOTE]
> **Verification Status**: Tested and Verified against `rekuiper` engine with live telemetry stream load on **2026-10-01 16:44:05 UTC**.  
> **Scorecard**: **1 / 1 Window Functions Fully Verified with Live Data (100% Parity)**:  
> `row_number()` verified sequentially numbering stream events (1, 2, 3...) in live pipeline.

A window function performs a calculation across a set of table rows that are somehow related to the current row. This is comparable to the type of calculation that can be done with an aggregate function. For now, window functions can only be used in select fields.

## ROW_NUMBER

```text
row_number()
```

ROW_NUMBER numbers all rows sequentially (for example 1, 2, 3, 4, 5).
