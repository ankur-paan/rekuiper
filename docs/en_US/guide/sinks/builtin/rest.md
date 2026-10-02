# REST Action

The REST action publishes output messages to a RESTful API endpoint.

## Properties

| Property name | Optional | Description |
|---|---|---|
| method | true | The HTTP method for the RESTful API. Case-insensitive string: `"get"`, `"post"`, `"put"`, `"patch"`, `"delete"`, or `"head"`. Default: `"get"`. |
| url | false | The RESTful API endpoint URL, such as `https://www.example.com/api/dummy`. |
| bodyType | true | The request body type: `"none"`, `"json"`, `"text"`, `"html"`, `"xml"`, `"javascript"`, `"form"`, `"binary"`, or `"formdata"`. For `"get"` and `"head"`, no body is required, so the default value is `"none"`. For other HTTP methods, the default value is `"json"`. For `"html"`, `"xml"`, and `"javascript"`, configure `dataTemplate` carefully to ensure correct formatting. |
| timeout | true | The HTTP request timeout in milliseconds. Default: 5000 ms. |
| headers | true | Additional HTTP headers sent with the request. |
| formdata | true | Key-value pairs for form data when `bodyType` is `formdata`. The encoded body bytes are transmitted as a file. Each key-value pair represents one part of the multipart form. |
| fileFieldName | true | The form field name used when uploading files with `multipart/form-data`. |
| debugResp | true | Controls whether response information is printed to the console log. If `true`, rekuiper prints the response. If `false`, rekuiper skips logging. Default: `false`. |
| certificationPath | true | The certificate file path. Can be an absolute path or a relative path. For a relative path, the base path is the execution directory of the `kuiperd` command. For example, if you run `bin/kuiperd` from `/var/kuiper`, the base path is `/var/kuiper`. If you run `./kuiperd` from `/var/kuiper/bin`, the base path is `/var/kuiper/bin`. |
| privateKeyPath | true | The private key file path. Can be an absolute path or a relative path, same as `certificationPath`. |
| rootCaPath | true | The root CA file path. Can be an absolute path or a relative path, same as `certificationPath`. |
| tlsMinVersion | true | Specifies the minimum TLS protocol version negotiated with the client. Accepted values: `tls1.0`, `tls1.1`, `tls1.2`, and `tls1.3`. Default: `tls1.2`. |
| renegotiationSupport | true | Controls how the client handles server-initiated renegotiation requests. Supported values: `never`, `once`, or `freely`. Default: `never`. |
| insecureSkipVerify | true | Controls whether to skip certificate verification. If `true`, certificate verification is skipped. If `false`, certificates are verified. Default: `true`. |
| oAuth | true | Defines the OAuth authentication flow. For simpler schemes like API keys, configure headers directly without this block. Refer to [OAuth configuration](../../sources/builtin/http_pull.md#OAuth) in the HTTP pull source documentation for more information. |

Other common sink properties are supported. Refer to [sink common properties](../overview.md#common-properties) for more information.

::: v-pre
REST services frequently require a specific data format. Use the common sink property `dataTemplate` to format the payload. Refer to the [data template documentation](../data_template.md).

The following sample configuration connects to the EdgeX Foundry core command service. The data template <code v-pre>{{.key}}</code> outputs the value of `key`. The template selects only field `key` in the result and changes the field name to `newKey`. Set `sendSingle` to `true` to send each element individually when the result is an array.
:::

```json
{
  "rest": {
    "url": "http://127.0.0.1:59882/api/v1/device/cc622d99-f835-4e94-b5cb-b1eff8699dc4/command/51fce08a-ae19-4bce-b431-b9f363bba705",
    "method": "post",
    "dataTemplate": "\"newKey\":\"{{.key}}\"",
    "sendSingle": true
  }
}
```

Example of OAuth authentication:

::: v-pre
OAuth header templates support only the following placeholders: <code v-pre>{{.access_token}}</code>, <code v-pre>{{.refresh_token}}</code>, <code v-pre>{{.token_type}}</code>, <code v-pre>{{.id_token}}</code>, and <code v-pre>{{.expires_in}}</code>. Names must match the JSON fields returned by the token endpoint. Other placeholders, such as <code v-pre>{{.message}}</code>, <code v-pre>{{.scope}}</code>, and <code v-pre>{{.custom_token}}</code>, evaluate only against the rule output and cannot read fields from the token response. Token endpoints must return the supported field names required by the configured headers. A single header cannot combine an OAuth placeholder with a rule-output template, but different headers can use OAuth and rule-output templates separately.
:::

```json
{
  "id": "ruleFollowBack",
  "sql": "SELECT follower FROM followStream",
  "actions": [{
    "rest": {
      "url": "https://com.awebsite/follows",
      "method": "POST",
      "sendSingle": true,
      "bodyType": "json",
      "dataTemplate": "{\"data\":{\"relationships\":{\"follower\":{\"data\":{\"type\":\"users\",\"id\":\"1398589\"}},\"followed\":{\"data\":{\"type\":\"users\",\"id\":\"{{.follower}}\"}}},\"type\":\"follows\"}}",
      "headers": {
        "Content-Type": "application/vnd.api+json",
        "Authorization": "Bearer {{.access_token}}"
      },
      "oAuth": {
        "access": {
          "url": "https://com.awebsite/oauth/token",
          "body": "{\"grant_type\": \"password\",\"username\": \"user@gmail.com\",\"password\": \"mypass\"}",
          "expire": "3600"
        }
      }
    }
  }]
}
```

## Visualization Mode

Create rule SQL and actions by using the graphical user interface.

## Text Mode

Create rule SQL and actions by using JSON definitions.

The following example sends data to TDengine by using its REST API:

```json
{
  "id": "rest1",
  "sql": "SELECT tele[0].Tag00001 AS temperature, tele[0].Tag00002 AS humidity FROM demoStream",
  "actions": [
    {
      "rest": {
        "bodyType": "text",
        "dataTemplate": "insert into mqtt.kuiper values (now, {{.temperature}}, {{.humidity}})",
        "debugResp": true,
        "headers": {"Authorization": "Basic cm9vdDp0YW9zZGF0YQ=="},
        "method": "POST",
        "sendSingle": true,
        "url": "http://xxx.xxx.xxx.xxx:6041/rest/sql"
      }
    }
  ]
}
```

## Configure Dynamic Properties

You can send data to dynamic URLs and configurations through the REST sink. The properties `method`, `url`, `bodyType`, and `headers` support dynamic values through template syntax.

The following example shows how to configure dynamic request parameters. When incoming data contains HTTP metadata, modify the SQL query to include those fields in the output:

```json
{
  "method": "post",
  "url": "http://xxx.xxx.xxx.xxx:6041/rest/sql",
  "temperature": 20,
  "humidity": 80
}
```

In the action configuration, use template syntax to assign `method` and `url` from the result fields:

```json
{
  "id": "rest2",
  "sql": "SELECT tele[0]->Tag00001 AS temperature, tele[0]->Tag00002 AS humidity, method, concat(\"http://xxx.xxx.xxx.xxx:6041/rest/sql\", urlPostfix) as url FROM demoStream",
  "actions": [
    {
      "rest": {
        "bodyType": "text",
        "dataTemplate": "insert into mqtt.kuiper values (now, {{.temperature}}, {{.humidity}})",
        "debugResp": true,
        "headers": {"Authorization": "Basic cm9vdDp0YW9zZGF0YQ=="},
        "method": "{{.method}}",
        "sendSingle": true,
        "url": "{{.url}}"
      }
    }
  ]
}
```

## File Upload

To upload data as files to an HTTP server, set `bodyType` to `formdata`.

### Key Characteristics

- Uses the `multipart/form-data` content type.
- Uploads binary results as form file content.
- Configures additional form attributes through `formData`.

### Best Practices

- For high-frequency data sources, configure batching or window aggregation to prevent frequent small file uploads.

### Example Configuration

```json
{
  "id": "restUpload",
  "sql": "SELECT value1, value2 FROM demoStream",
  "actions": [
    {
      "rest": {
        "url": "http://yoururlhere.com",
        "method": "post",
        "fileFieldName": "file1",
        "formData": {
          "key1": "value1",
          "key2": "value2"
        },
        "batchSize": 10,
        "format": "delimited",
        "sendSingle": true
      }
    }
  ]
}
```

In this example, the format `delimited` encodes content into CSV format with 10 records per upload.
