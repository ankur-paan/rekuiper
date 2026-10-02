# Execute AI Algorithms with Python Function Plugins

By integrating rekuiper and TensorFlow Lite, you can analyze streaming data by using pre-trained machine learning models. This tutorial explains how to build a Python portable plugin that classifies streaming images captured by edge devices.

You can download the completed plugin archive and source code from the [eKuiper resources repository](https://github.com/lf-edge/ekuiper/blob/master/docs/resources/pyai.zip).

## Prerequisites

Download a trained TensorFlow Lite model before starting. This tutorial uses the model from the [TensorFlow image classification example](https://www.tensorflow.org/lite/examples/image_classification/overview).

Prepare the following environment:

1. Install Python 3.x.
2. Install required packages:

   ```shell
   pip install pynng ekuiper tflite_runtime
   ```

By default, rekuiper starts portable plugins by using the `python` command. If your environment requires `python3`, configure the command name in the [portable plugin configuration file](../../configuration/global_configurations.md#portable-plugin-configurations).

When building with Docker, use the `lfedge/ekuiper:<tag>-slim-python` container image, which includes both rekuiper and the Python runtime.

## Develop the Plugin

We will develop a function plugin named `labelImage`. The function takes binary image data as input and returns a string representing the recognized label. For example, when an image contains a peacock, `labelImage(col)` outputs `peacock`.

### Implement the Inference Logic

1. Download the [Image Classification Model archive](https://storage.googleapis.com/download.tensorflow.org/models/tflite/mobilenet_v1_1.0_224_quant_and_labels.zip), extract its contents, and place `mobilenet_v1_1.0_224.tflite` and `labels.txt` in your project folder.
2. Create `label.py` and implement the `label(file_bytes)` function:

```python
import base64
import json
import tflite_runtime.interpreter as tf

def label(file_bytes):
    # Load the model
    interpreter = tf.Interpreter(model_path="mobilenet_v1_1.0_224.tflite")
    interpreter.allocate_tensors()

    input_details = interpreter.get_input_details()
    output_details = interpreter.get_output_details()

    # Preprocess image bytes and populate input tensors (omitted for brevity)

    interpreter.set_tensor(input_details[0]['index'], input_data)
    interpreter.invoke()
    output_data = interpreter.get_tensor(output_details[0]['index'])

    # Post-process probabilities and return label results
    return result
```

You can test the logic independently by adding a test script:

```python
if __name__ == '__main__':
    with open("peacock.jpg", "rb") as f:
        result = label(base64.b64encode(f.read()))
        print(json.dumps(result))
```

Expected output ranked by confidence:

```json
[
  {"confidence": 0.9999935626983643, "label": "85:peacock"},
  {"confidence": 2.156877371817245e-06, "label": "8:cock"},
  {"confidence": 1.5930896779536852e-06, "label": "81:black grouse"}
]
```

### Implement the Plugin Interface

Create `label_func.py` to wrap the inference logic in the rekuiper Python plugin SDK:

```python
from typing import List, Any
from ekuiper import Function, Context
from label import label

class LabelImageFunc(Function):
    def __init__(self):
        pass

    def validate(self, args: List[Any]):
        if len(args) != 1:
            return "invalid argument length: expected 1 argument"
        return ""

    def exec(self, args: List[Any], ctx: Context):
        return label(args[0])

    def is_aggregate(self):
        return False

labelIns = LabelImageFunc()
```

Create a function metadata descriptor named `functions/labelImage.json` to enable user interface discovery in eKuiper manager.

### Package the Plugin

1. Create `requirements.txt` listing all Python dependencies, and create an installation script named `install.sh`:

   ```shell
   #!/bin/sh
   cur=$(dirname "$0")
   pip install -r "$cur/requirements.txt"
   ```

2. Create an entry file named `pyai.py`:

   ```python
   from ekuiper import PluginConfig, plugin
   from label_func import labelIns

   if __name__ == '__main__':
       c = PluginConfig("pyai", {}, {}, {"labelImage": lambda: labelIns})
       plugin.start(c)
   ```

3. Create the plugin metadata file named `pyai.json`:

   ```json
   {
     "version": "v1.0.0",
     "language": "python",
     "executable": "pyai.py",
     "sources": [],
     "sinks": [],
     "functions": [
       "labelImage"
     ]
   }
   ```

Package all files into a ZIP archive with the following structure:

- `label.py`
- `label_func.py`
- `requirements.txt`
- `mobilenet_v1_1.0_224.tflite`
- `labels.txt`
- `install.sh`
- `pyai.py`
- `pyai.json`
- `functions/`
  - `labelImage.json`

## Install the Plugin

Upload the ZIP package to the rekuiper host and install it through the REST API:

```http
POST http://localhost:9081/plugins/portables
Content-Type: application/json

{
  "name": "pyai",
  "file": "file:///tmp/pyai.zip"
}
```

## Run the Plugin in Rules

### Create the Stream

Define a stream that accepts binary payloads on MQTT topic `tfdemo`:

```http
POST http://localhost:9081/streams
Content-Type: application/json

{
  "sql": "CREATE STREAM tfdemo () WITH (DATASOURCE=\"tfdemo\", FORMAT=\"BINARY\")"
}
```

### Create the Rule

Create a rule that extracts the top classification label and publishes results to topic `ekuiper/labels`:

```http
POST http://localhost:9081/rules
Content-Type: application/json

{
  "id": "ruleTf",
  "sql": "SELECT labelImage(self)[0]->label as label FROM tfdemo",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "sendSingle": true,
        "topic": "ekuiper/labels"
      }
    }
  ]
}
```

### Publish Input Data

The following Go program publishes test images to the `tfdemo` topic:

```go
package main

import (
    "fmt"
    "os"
    "time"
    mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {
    const TOPIC = "tfdemo"
    images := []string{
        "peacock.png",
        "frog.jpg",
    }
    opts := mqtt.NewClientOptions().AddBroker("tcp://localhost:1883")
    client := mqtt.NewClient(opts)
    if token := client.Connect(); token.Wait() && token.Error() != nil {
        panic(token.Error())
    }
    for _, image := range images {
        fmt.Println("Publishing " + image)
        payload, err := os.ReadFile(image)
        if err != nil {
            fmt.Println(err)
            continue
        }
        if token := client.Publish(TOPIC, 0, false, payload); token.Wait() && token.Error() != nil {
            fmt.Println(token.Error())
        } else {
            fmt.Println("Published " + image)
        }
        time.Sleep(1 * time.Second)
    }
    client.Disconnect(0)
}
```

### Verify Output

Subscribe to MQTT topic `ekuiper/labels` to verify predictions:

```json
{"label": "85:peacock"}
{"label": "33:tailed frog, bell toad, ribbed toad, tailed toad, Ascaphus trui"}
```

## Conclusion

Python function plugins allow you to integrate machine learning inference into real-time SQL streaming rules. You can replace the demonstration model with custom models to support diverse edge AI workloads.
