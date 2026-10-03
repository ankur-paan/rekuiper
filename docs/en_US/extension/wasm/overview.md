# WebAssembly (Wasm) Plugins

WebAssembly (Wasm) plugins provide safe, sandboxed function extensions for `rekuiper`.

You can write plugins in any language that compiles to WebAssembly bytecode. Supported languages include Rust, C, C++, and Go.

`rekuiper` contains an embedded WebAssembly interpreter. You do not need to install external runtimes or system libraries.

## Development Workflow

Follow these steps to create a Wasm plugin:
1. Write your function logic in Rust, C, or Go.
2. Compile the source code to a WebAssembly module (`.wasm`).
3. Place or register the module in `rekuiper`.
4. Call your function in SQL rules.

---

## 1. Implement the Function

### Example in Rust

Create a simple Rust library project:

```bash
cargo new --lib fibonacci_wasm
```

In `Cargo.toml`, set the crate type to `cdylib`:

```toml
[lib]
crate-type = ["cdylib"]
```

Implement the calculation in `src/lib.rs`:

```rust
#[no_mangle]
pub extern "C" fn fib(n: i32) -> i32 {
    if n <= 1 {
        return n;
    }
    fib(n - 1) + fib(n - 2)
}
```

Compile the project to the WebAssembly target:

```bash
cargo build --target wasm32-unknown-unknown --release
```

The output file is at `target/wasm32-unknown-unknown/release/fibonacci_wasm.wasm`.

### Example in Go (TinyGo)

Create `fibonacci.go`:

```go
package main

func main() {}

//export fib
func fib(n int32) int32 {
    if n <= 1 {
        return n
    }
    return fib(n-1) + fib(n-2)
}
```

Compile using TinyGo:

```bash
tinygo build -o fibonacci.wasm -target wasm fibonacci.go
```

---

## 2. Plugin Installation

Install the plugin with the REST API using a file path or base64 bytecode:

```http
POST http://localhost:9081/plugins/wasm
Content-Type: application/json

{
  "name": "fibonacci",
  "file": "file:///plugins/wasm/fibonacci.wasm"
}
```

Or provide raw bytecode encoded with base64:

```http
POST http://localhost:9081/plugins/wasm
Content-Type: application/json

{
  "name": "fibonacci",
  "bytecode": "AGFzbQEAAAA..."
}
```

The engine registers all exported functions as SQL functions automatically.

### List WebAssembly Plugins

Send a `GET` request to list all installed WebAssembly plugins:

```http
GET http://localhost:9081/plugins/wasm
```

Response sample:

```json
[
  {
    "name": "fibonacci",
    "functions": ["fib"]
  }
]
```

### Delete a WebAssembly Plugin

Send a `DELETE` request to remove an installed WebAssembly plugin:

```http
DELETE http://localhost:9081/plugins/wasm/fibonacci
```

The engine unloads the module and removes all registered functions immediately.

You can also place `.wasm` files directly into the `plugins/wasm` directory. `rekuiper` loads them on startup.

---

## 3. Query Execution

You can execute WebAssembly functions in SQL rules in two ways:

### Method A: Direct Function Call

When a plugin is registered, its exported functions become available directly:

```sql
SELECT fib(num) AS fib_result FROM sensor_stream;
```

### Method B: Generic `wasm_run` Function

You can also call the generic `wasm_run` scalar function:

```sql
SELECT wasm_run('fib', num) AS fib_result FROM sensor_stream;
```

---

## Performance and Isolation

The embedded Wasm engine offers strong operational guarantees:
- **Memory Safety**: Each module runs in an isolated linear memory space. A crash in a plugin cannot corrupt the `rekuiper` process.
- **Portability**: The same `.wasm` binary runs identically on x86_64, ARMv7, and AArch64 edge hardware.
