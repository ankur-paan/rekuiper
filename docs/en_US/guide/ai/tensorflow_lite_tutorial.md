# Execute TensorFlow Lite Models with Native Plugins (Unsupported)

> [!WARNING]
> **Status: Unsupported in rekuiper**
>
> Native Go dynamic plugins (`.so` compiled by using `go build -buildmode=plugin`) are not supported in rekuiper. A future release may introduce alternative implementations through native Rust bindings or WebAssembly modules.

## Upstream Documentation

Refer to the upstream repository for original implementation details:

- **Upstream Tutorial:** [eKuiper TensorFlow Lite Tutorial](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/guide/ai/tensorflow_lite_tutorial.md)
- **Upstream Source Code:** [labelImage.go](https://github.com/lf-edge/ekuiper/blob/master/extensions/functions/labelImage/labelImage.go)

## Supported Alternatives in rekuiper

Use the following portable mechanisms for on-device inference:

1. **Python Extensions**: [Execute TensorFlow Lite Models with Python Plugins](./python_tensorflow_lite_tutorial.md)
2. **External Function Services**: [Execute TensorFlow Lite Models with External gRPC Services](./tensorflow_lite_external_function_tutorial.md)
3. **OpenVINO Inference**: [Execute OpenVINO Models with Python Plugins](./python_openvino_tutorial.md)
4. **ONNX Runtime**: [Execute ONNX Models with the Function Plugin](./onnx.md)
