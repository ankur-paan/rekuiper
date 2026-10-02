# REST API Authentication

rekuiper supports `JWT RSA256` authentication for management REST APIs when enabled.

To enable authentication:
1. Place your RSA public key file in the `etc/mgmt` directory.
2. Sign your JWT tokens using the matching RSA private key.
3. Include the token in HTTP request headers:

```http
Authorization: <JWT_TOKEN>
```

If the token validates successfully, the server processes the request. If validation fails, the server returns HTTP status code `401 Unauthorized`.

## JWT Header

```json
{
  "typ": "JWT",
  "alg": "RS256"
}
```

## JWT Payload Claims

The JWT payload must use the following claim schema:

| Claim | Required | Description |
| :--- | :--- | :--- |
| `iss` | Yes | Issuer. Must match the filename of the public key stored in `etc/mgmt`. |
| `aud` | Yes | Audience. Must be set to `eKuiper`. |
| `exp` | No | Expiration timestamp in Unix epoch seconds. |
| `jti` | No | Unique JWT identifier. |
| `iat` | No | Issuance timestamp in Unix epoch seconds. |
| `nbf` | No | Not-before timestamp in Unix epoch seconds. |
| `sub` | No | Subject identifier. |

Example payload:

```json
{
  "iss": "sample_key.pub",
  "aud": "eKuiper"
}
```

Ensure that the public key file `sample_key.pub` exists in the `etc/mgmt` directory.

## JWT Signature

Sign the token using your RSA private key. rekuiper verifies the signature against the matching public key located in `etc/mgmt`.
