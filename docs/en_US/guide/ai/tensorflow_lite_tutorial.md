# Run TensorFlow Lite Model with Go Native Plugin (Unsupported)

> [!WARNING]
> **Status: Unsupported in rekuiper**
> 
> Native Go C-shared dynamic plugins (`.so` compiled via `go build -buildmode=plugin`) are **unsupported in rekuiper as of now**. This capability may be added or revisited in a future release (for example via native Rust bindings or WebAssembly).

## Upstream eKuiper Documentation

The original tutorial and Go source implementation for the native TensorFlow Lite plugin can be found at the upstream repository:

- **Upstream eKuiper Tutorial:** [eKuiper TensorFlow Lite Tutorial](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/guide/ai/tensorflow_lite_tutorial.md)
- **Upstream Plugin Source:** [labelImage.go](https://github.com/lf-edge/ekuiper/blob/master/extensions/functions/labelImage/labelImage.go)

## Supported Alternatives in rekuiper

For on-device AI/ML inference with rekuiper, please use the supported portable mechanisms:

1. **Python Extension**: [Run TensorFlow Lite Model with Python Plugin](./python_tensorflow_lite_tutorial.md)
2. **External Function Service**: [Run TensorFlow Lite with External gRPC Service](./tensorflow_lite_external_function_tutorial.md)
3. **OpenVINO Inference**: [Run OpenVINO Models with Python Plugin](./python_openvino_tutorial.md)
4. **ONNX Runtime**: [Run ONNX Model with rekuiper](./onnx.md)
