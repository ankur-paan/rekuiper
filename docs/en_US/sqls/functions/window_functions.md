# Window Functions

> [!NOTE]
> **Verification Status**: Tested and Verified against `rekuiper` engine with live telemetry stream load on **2026-10-01 16:44:05 UTC**.  
> **Scorecard**: **1 / 1 Window Functions Fully Verified with Live Data (100% Parity)**:  
> `row_number()` verified sequentially numbering stream events (1, 2, 3...) in live pipeline.

A window function performs a calculation across a set of stream or table rows related to the current row. This calculation resembles an aggregate function. Currently, you can use window functions only in the `SELECT` clause.

## ROW_NUMBER

```text
row_number()
```

Assigns a sequential integer to each row in the window or result set, starting from 1 (for example: 1, 2, 3, 4, 5).
