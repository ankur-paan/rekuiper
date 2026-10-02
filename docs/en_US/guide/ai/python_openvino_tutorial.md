# Execute OpenVINO Algorithms with Python Function Plugins

[OpenVINO](https://www.intel.com/content/www/us/en/developer/tools/openvino-toolkit/overview.html) is an open-source toolkit that optimizes and deploys machine learning models across heterogeneous hardware environments. It converts and accelerates models trained in frameworks such as TensorFlow, PyTorch, and Caffe.

This tutorial demonstrates how to build an edge defect detection system using rekuiper and OpenVINO, based on the [Intel Industrial Surface Defect Detection Reference Implementation](https://www.intel.com/content/www/us/en/developer/articles/reference-implementation/industrial-surface-defect-detection.html).

You can download the completed plugin archive and source code from the [eKuiper resources repository](https://github.com/lf-edge/ekuiper/blob/master/docs/resources/openvinoProject.zip).

## Prerequisites

Prepare the following environment before starting:

1. Install Python 3.x, and configure `pythonBin` under the portable plugin settings in [rekuiper configuration](../../configuration/global_configurations.md#portable-plugin-configurations).
2. Install required Python packages:

   ```shell
   pip install opencv-python==4.7.0.* openvino==2023.0.0 numpy==1.24.3
   ```

When using Docker, select the <span v-pre>`lfedge/ekuiper:{{tag}}-slim-python`</span> container image, which includes both rekuiper and Python. Install any additional libraries inside the container or by extending the Dockerfile.

## Develop the Plugin

We will develop a custom function plugin that accepts Base64-encoded image data and outputs a JSON object containing defect counts, processed image bytes, and inference latency.

### Implement the Inference Logic

1. Place `model.xml` and `model.bin` in the `models` directory.
2. Create `inference.py` and implement the inference pipeline:

```python
import base64
import json
import cv2
import numpy as np
from time import time
from openvino.inference_engine import IECore

def inference(file_bytes):
    ie = IECore()
    net = ie.read_network(model="models/model.xml", weights="models/model.bin")

    input_blob = next(iter(net.input_info))
    output_blob = next(iter(net.outputs))
    n, c, h, w = net.input_info[input_blob].input_data.shape

    exec_net = ie.load_network(network=net, device_name="CPU")

    t0 = time()
    img_str = base64.b64decode(file_bytes.encode("ascii"))
    ndarray = np.fromstring(img_str, np.uint8)
    frame = cv2.imdecode(ndarray, cv2.IMREAD_COLOR)
    frame = cv2.resize(frame, (w, h))
    org_img = frame.copy()
    frame = frame.transpose((2, 0, 1))
    images = np.expand_dims(frame, axis=0)

    pred = exec_net.infer(inputs={input_blob: images})
    infer_time = (time() - t0) * 1000

    result = np.squeeze(pred[output_blob])
    result[result < 0.5] = 0
    result[result >= 0.5] = 255
    result = result.astype(np.uint8)

    contours, _ = cv2.findContours(result, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)

    pred_mask = np.zeros((h, w, 3), dtype=np.uint8)
    pred_mask[result < 128] = (0, 0, 0)
    pred_mask[result >= 128] = (255, 255, 255)

    base64_str = cv2.imencode('.jpg', np.hstack((org_img, pred_mask)))[1].tobytes()
    b64str = base64.b64encode(base64_str).decode()

    return {
        "inference time": infer_time,
        "defect": len(contours),
        "base64": b64str
    }
```

You can test the inference logic independently with a sample image:

```python
if __name__ == '__main__':
    with open("test.jpg", "rb") as f:
        result = inference(base64.b64encode(f.read()).decode())
        print(json.dumps(result))
```

### Implement the Plugin Interface

Create `inference_func.py` to wrap the inference logic using the rekuiper Python plugin SDK:

```python
from typing import List, Any
import logging
from ekuiper import Function, Context
from inference import inference

class InferenceFunc(Function):
    def __init__(self):
        pass

    def validate(self, args: List[Any]):
        if len(args) != 1:
            return "invalid argument length: expected 1 argument"
        return ""

    def exec(self, args: List[Any], ctx: Context):
        logging.debug("executing OpenVINO inference")
        return inference(args[0])

    def is_aggregate(self):
        return False

inferenceIns = InferenceFunc()
```

Create a function metadata descriptor named `functions/defect.json`.

### Package the Plugin

1. Create `requirements.txt` listing dependencies, and create an installation script named `install.sh`:

   ```shell
   #!/bin/sh
   cur=$(dirname "$0")
   pip install -r "$cur/requirements.txt"
   ```

2. Create an entry file named `main.py`:

   ```python
   from ekuiper import PluginConfig, plugin
   from inference_func import inferenceIns

   if __name__ == '__main__':
       c = PluginConfig("defect", {}, {}, {"inference": lambda: inferenceIns})
       plugin.start(c)
   ```

3. Create the plugin metadata descriptor `defect.json`:

   ```json
   {
     "version": "v1.0.0",
     "language": "python",
     "executable": "main.py",
     "sources": [],
     "sinks": [],
     "functions": [
       "inference"
     ]
   }
   ```

Package all files into a ZIP archive with the following structure:

- `inference.py`
- `inference_func.py`
- `requirements.txt`
- `install.sh`
- `main.py`
- `defect.json`
- `models/`
  - `model.bin`
  - `model.xml`
- `functions/`
  - `defect.json`

## Install the Plugin

Upload the ZIP package to the rekuiper host and install it through the REST API:

```http
POST http://localhost:9081/plugins/functions
Content-Type: application/json

{
  "name": "defect",
  "file": "file:///tmp/defect.zip"
}
```

## Run the Plugin in Rules

### Create the Stream

Define an input stream for incoming image payloads:

```http
POST http://localhost:9081/streams
Content-Type: application/json

{
  "sql": "CREATE STREAM openvino_demo () WITH (DATASOURCE=\"openvino_demo\")"
}
```

### Create the Rule

Create a rule that executes defect segmentation on images and publishes results to topic `ekuiper/defect`:

```http
POST http://localhost:9081/rules
Content-Type: application/json

{
  "id": "ruleOp",
  "sql": "SELECT image AS origin, inference(image)->base64 AS afterProcess FROM openvino_demo WHERE inference(image)->defect >= 0",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "sendSingle": true,
        "topic": "ekuiper/defect"
      }
    }
  ]
}
```

### Publish Input Data

The following Python script sends test image frames to the `openvino_demo` topic:

```python
import base64
import json
import time

def publish(client):
    topic = "openvino_demo"
    for _ in range(5):
        time.sleep(1)
        encoded = base64.b64encode(open('./1.png', 'rb').read()).decode()
        payload = json.dumps({"image": encoded})
        result = client.publish(topic, payload)
        if result[0] == 0:
            print(f"Sent payload to topic {topic}")
        else:
            print(f"Failed to publish to topic {topic}")
```

### Verify Results

Subscribe to topic `ekuiper/defect` to receive processed images with defect overlays whenever the model detects anomalies.

## Conclusion

This tutorial demonstrated how to build a Python portable plugin that runs OpenVINO deep learning defect segmentation in real-time streaming queries.
