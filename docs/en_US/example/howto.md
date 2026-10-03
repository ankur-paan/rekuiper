# Step-by-Step Guide: Navigating rekuiper with the Management Console UI

This document describes how to run the cookbook examples with rekuiper. Before you run the examples, [install rekuiper](../installation.md).

You can define rules with SQL in the example scenarios. You can manage and execute rules with the rekuiper manager UI, the REST API, or the command-line interface.

## Data Preparation

Each example includes sample input data. You can also supply custom data. If you have active data sources, you can record live data to a file through a rekuiper rule and replay that file in the examples.

### Record Data

rekuiper supports diverse source protocols and provides a File sink. You can save live stream data to a local file through a rule. For example, you can record MQTT data with the rekuiper manager UI. Before you begin, [install and configure the rekuiper manager](../installation.md#running-ekuiper-with-management-console).

1. **Create Data Source**: In the navigation sidebar, click **Streams** to open the stream management page. Click **Create stream** (or select the **Visual** designer tab). Set the stream name to `mqttDemoStream`. Under **Connector & Serialization**, select connector type `mqtt` and set the topic to `demo/#`. Notice the live **Generated SQL** preview panel updates in real time: `CREATE STREAM mqttDemoStream () WITH (TYPE="mqtt", DATASOURCE="demo/#")`. Click **Create stream** to submit the stream to eKuiper.

   ![record_stream.png](./resources/record_stream.png)

2. **Create and Run the Rule**: In the navigation sidebar, click **Rules**, then click **Create rule**. In the multi-step rule designer:
   - **Step 1 (Basics)**: Set the **Rule ID** to `ruleRecordToFile` (with optional description or tags).
   - **Step 2 (Query)**: In the SQL editor, enter the query `SELECT * FROM mqttDemoStream`. The editor provides syntax validation and schema hints directly from the active eKuiper node.

   ![record_sql.png](./resources/record_sql.png)

   - **Step 3 (Outputs)**: Under **Add an output**, select the **File** sink card. Configure the output properties: set **File path** (`path`) to `data/mock.lines`, set **File type** (`fileType`) to `lines`, and set **Check Interval** (`checkInterval`) to `10000`. Click **Validate with eKuiper** to verify schema and engine compatibility, then click **Create rule**. The rule starts running immediately upon creation.

   ![record_action.png](./resources/record_action.png)

   On the rule list or details page, verify that the rule status displays **Running**. You can monitor real-time throughput metrics, inspect the execution topology, or manage the rule lifecycle (Start, Stop, Restart).

3. **Inspect Data**: The rule writes output lines to `data/mock.lines`. The file stores one JSON string per line. You can view or modify the file in any text editor.

## Run Examples

For testing and debugging, use a File source as input. When the example executes successfully, replace the File source with your production source.

Most cookbook examples follow these conventions:

- The rule reads from a File source stream named `demoStream`. This setup lets you replay recorded test data easily.
- The rule publishes results to an MQTT sink topic: <code v-pre>result/{{ruleId}}</code>.

Follow these steps to run an example in the management console:

1. **Prepare Data**: Copy sample data from the example page into a text file, or [record live data](#record-data).
2. **Upload Data**: In the navigation sidebar, click **Files** (or open the **Uploads** page). Upload your data file (such as `mock.lines`) to eKuiper's configuration storage by dragging and dropping or selecting the file.
3. **Create File Stream**: In the navigation sidebar, click **Streams**, then click **Create stream**. Set the stream name to `demoStream`, select stream type `file`, and set the data source to the uploaded file name (`mock.lines`).

   ![replay_source.png](./resources/replay_source.png)

   If you require custom stream configuration keys (such as setting custom directories or formats), click **Configuration Key**, name the configuration (e.g. `linesInUpload`), and define parameters such as file path `data/uploads/` and format `lines`.

   ![replay_conf.png](./resources/replay_conf.png)

   Click **Create stream** to submit the stream definition to eKuiper.

4. **Subscribe to Result Topic**: Open an MQTT client such as [MQTTX](https://mqttx.app/). Subscribe to the topic <code v-pre>result/{{ruleId}}</code> to view rule output.
5. **Create Rule**: In the navigation sidebar, click **Rules**, then click **Create rule**. Set the **Rule ID** to `ruleX`, and enter the example SQL query (e.g. `SELECT * FROM demoStream`) in the SQL query editor.

   ![replay_sql.png](./resources/replay_sql.png)

   Under **Outputs**, click **Add an output** and select the **MQTT** sink card. Set the broker address (such as `tcp://yourbroker:1883`) and target topic to <code v-pre>result/{{ruleId}}</code> (or `result/rulex`).

   ![replay_action.png](./resources/replay_action.png)

   Click **Validate with eKuiper**, then click **Create rule**. In the rule list, verify that the rule status displays **Running**. You can click the status badge to inspect message throughput, latency, and processing health.

6. **Inspect Output**: View the rule results in your MQTT client.

## Summary

This document describes how to execute cookbook examples with the rekuiper manager UI. You can also use the REST API or the CLI to deploy streams and rules. You can adjust the example SQL queries to test custom processing logic.
