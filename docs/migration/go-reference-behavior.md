# Go Reference Behavior

This document freezes the current Go implementation behavior that the Rust migration must match before cutover.

## HTTP v1 behavior

The current service exposes a single write endpoint:

- `POST /update_asset`

Observed behavior from the existing test suite:

| Scenario | Status | Response body |
| --- | --- | --- |
| Invalid `User-Agent` when auth prefix is enabled | `401` | `{"message":"Invalid User-Agent"}` |
| Invalid bearer token when auth token is enabled | `401` | `{"message":"Invalid authorization token"}` |
| Invalid JSON payload | `400` | `{"message":"Invalid request payload","error":"..."}` |
| Unknown server region | `400` | `{"message":"Server region not found in configuration"}` |
| Region configured but disabled | `503` | `{"message":"Asset updater for this region is not enabled","server":"..."}` |
| Accepted request | `200` | `{"message":"Asset updater started running","server":"..."}` |

The Go server does not currently expose `/health` or `/healthz`, even though the checked-in Docker Compose file expects one.

## Download record format

The reference downloaded-assets record is a JSON object:

```json
{
  "bundle/name": "hash-or-crc-string"
}
```

Rules:

- Keys are bundle paths.
- Values are bundle hash strings for JP/EN and CRC-derived strings for TW/KR/CN.
- Missing file means "no assets downloaded yet".

## Frozen sample outputs

These sample outputs are the current parity baseline. The Rust codec smoke tests keep the same hashes inline.

### `0703.usm`

| Output | SHA-256 |
| --- | --- |
| `usm/0703.m2v` | `28392362da3cc9f837cbf7db160c423e00b7edc6d1fa3baad4e6252455db5804` |

### `se_0126_01.acb`

| Output | SHA-256 |
| --- | --- |
| `acb/se_0126_01.hca` | `9e4f11119803d743191fab904933e4c8a4a79229310592c3368d57f6f1d8c9fe` |
| `acb/se_0126_01_BGM.hca` | `34026ca6dff11f4fbba14c82c104b245cce51466b684e97b4f6798d3b294261c` |
| `acb/se_0126_01_SCREEN.hca` | `9e4f11119803d743191fab904933e4c8a4a79229310592c3368d57f6f1d8c9fe` |
| `acb/se_0126_01_VR.hca` | `9e4f11119803d743191fab904933e4c8a4a79229310592c3368d57f6f1d8c9fe` |
