# Transform Functions

> [!NOTE]
> **Verification Status**: Tested and Verified against `rekuiper` engine with live telemetry stream load on **2026-10-01 17:54:40 UTC**.  
> **Scorecard**: **11 Verified, 0 Unsupported, 0 Broken**:  
> - **Verified (11)**: `cast`, `convert_tz`, `to_seconds`, `encode` (base64), `decode` (base64), `compress` (zlib/gzip/flate/zstd), `decompress` (zlib/gzip/flate/zstd), `trunc`, `chr`, `hex2dec`, `dec2hex`.

Transform functions convert data types, encode payloads, and apply compression algorithms.

## CAST

```text
cast(col, dataType)
```

Converts a value to the specified target data type. Supported target types include: `bigint`, `float`, `string`, `boolean`, `bytea`, and `datetime`.

### Cast to datetime

When casting a value to `datetime`, the engine applies the following conversion rules:

1. If the input is already a `datetime` value, the function returns the value directly.
2. If the input is `bigint` or `float`, the function interprets the number as milliseconds since January 1, 1970 00:00:00 UTC.
3. If the input is `string`, the function parses the format into a `datetime` value. For supported time formats, refer to [TimeFormats](https://github.com/jinzhu/now/blob/f067b166b35a996b9ff5a0f610225e1458f23adc/main.go#L17-L27).
4. Conversions from other types are not supported.

## CONVERT_TZ

```text
convert_tz(col, "Asia/Shanghai")
```

Converts a timestamp to the specified target time zone. Specify time zones using the [IANA Time Zone Database](https://www.iana.org/time-zones) format. The default time zone is `UTC`. Specify `Local` to apply the host system time zone.

> [!NOTE]
> In Alpine Linux containers, you must install the `tzdata` package (for example, `apk add tzdata`) to provide time zone database files.

## TO_SECONDS

```text
to_seconds(col)
```

Converts `col` to a datetime value and returns Unix epoch time as the number of elapsed seconds since January 1, 1970 00:00:00 UTC.

## ENCODE

```text
encode(col, encodeType)
```

Encodes binary or string payload data into a string representation using the specified encoding scheme. Currently, the engine supports `"base64"`.

## DECODE

```text
decode(col, encodeType)
```

Decodes an encoded string into its original representation using the specified decoding scheme. Currently, the engine supports `"base64"`.

## COMPRESS

```text
compress(input, method)
```

Compresses an input string or binary payload using the specified algorithm. Supported methods include `'zlib'`, `'gzip'`, `'flate'`, and `'zstd'`.

## DECOMPRESS

```text
decompress(input, method)
```

Decompresses compressed payload data using the specified algorithm. Supported methods include `'zlib'`, `'gzip'`, `'flate'`, and `'zstd'`.

## TRUNC

```text
trunc(dec, int)
```

Truncates the decimal number `dec` to the number of decimal places specified by `int`. If the second argument is less than 0, the function uses 0. If greater than 34, the function uses 34. The function removes trailing zeros from the result.

## CHR

```text
chr(col)
```

Returns the ASCII character corresponding to the specified integer code.

## HEX2DEC

```text
hex2dec(col)
```

Converts a hexadecimal string into its decimal integer value. The argument must be a string. Values such as `"0x10"` or `"10"` convert to integer `16`.

## DEC2HEX

```text
dec2hex(col)
```

Converts a decimal integer into a hexadecimal string with a `"0x"` prefix. For example, integer `16` converts to `"0x10"`.
