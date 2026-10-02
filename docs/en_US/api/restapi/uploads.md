# File Uploads Management

The rekuiper REST API manages auxiliary and configuration file uploads. Uploaded files are stored in the `${dataPath}/uploads` directory and overwrite existing files with identical names. The API returns the absolute filesystem path for use in stream, sink, or rule configurations.

## Upload a Configuration File

You can upload files using three methods:

### Upload via Multipart Form Data

Send an HTTP POST request using `multipart/form-data`. The file input field must be named `uploadFile`:

```http
POST http://localhost:9081/config/uploads
Content-Type: multipart/form-data; boundary=----WebKitFormBoundary
```

Example HTML upload form:

```html
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <title>Upload File</title>
  </head>
  <body>
    <form
      enctype="multipart/form-data"
      action="http://127.0.0.1:9081/config/uploads"
      method="post"
    >
      <input type="file" name="uploadFile" />
      <input type="submit" value="upload" />
    </form>
  </body>
</html>
```

### Upload via Inline Text Content

Supply the target filename and file text content directly in JSON:

```http
POST http://localhost:9081/config/uploads
Content-Type: application/json

{
  "name": "my.json",
  "content": "{\"hello\":\"world\"}"
}
```

### Upload via Remote HTTP URL

Provide a target filename and an HTTP download URL:

```http
POST http://localhost:9081/config/uploads
Content-Type: application/json

{
  "name": "my.json",
  "file": "http://127.0.0.1:80/my.json"
}
```

## Show Uploaded Files

Use this endpoint to list all files stored in the `${dataPath}/uploads` directory:

```http
GET http://localhost:9081/config/uploads
```

Response sample:

```json
[
   "/ekuiper/data/uploads/zk.gif",
   "/ekuiper/data/uploads/abc.gif"
]
```

## Delete an Uploaded File

Use this endpoint to delete a file from the `${dataPath}/uploads` directory:

```http
DELETE http://localhost:9081/config/uploads/{fileName}
```
