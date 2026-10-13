// AWS Signature Version 4 for the console's requests and presigned links.
import { hex, hmacSha256, sha256, utf8 } from "./sha256.ts";

export interface Creds {
  accessKey: string;
  secretKey: string;
  region: string;
}

export const UNSIGNED = "UNSIGNED-PAYLOAD";

/** RFC 3986 encoding as SigV4 requires (encodeURIComponent leaves !'()* alone). */
export function awsEncode(s: string, keepSlash = false): string {
  const e = encodeURIComponent(s).replace(/[!'()*]/g, (c) => "%" + c.charCodeAt(0).toString(16).toUpperCase());
  return keepSlash ? e.replace(/%2F/g, "/") : e;
}

export function canonicalQuery(params: [string, string][]): string {
  return params
    .map(([k, v]) => [awsEncode(k), awsEncode(v)] as const)
    .sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0))
    .map(([k, v]) => `${k}=${v}`)
    .join("&");
}

function amzDate(d: Date): string {
  return d.toISOString().replace(/[:-]|\.\d{3}/g, "");
}

function signingKey(c: Creds, date: string): Uint8Array {
  let k = hmacSha256(utf8("AWS4" + c.secretKey), utf8(date));
  k = hmacSha256(k, utf8(c.region));
  k = hmacSha256(k, utf8("s3"));
  return hmacSha256(k, utf8("aws4_request"));
}

/** Sign a request; returns the headers to send. `path` must already be encoded. */
export function signHeaders(
  c: Creds,
  method: string,
  path: string,
  query: [string, string][],
  host: string,
  payloadHash = UNSIGNED,
  now = new Date(),
): Record<string, string> {
  const t = amzDate(now);
  const date = t.slice(0, 8);
  const scope = `${date}/${c.region}/s3/aws4_request`;
  const signed = "host;x-amz-content-sha256;x-amz-date";
  const creq = [
    method,
    path,
    canonicalQuery(query),
    `host:${host}\nx-amz-content-sha256:${payloadHash}\nx-amz-date:${t}\n`,
    signed,
    payloadHash,
  ].join("\n");
  const sts = ["AWS4-HMAC-SHA256", t, scope, hex(sha256(utf8(creq)))].join("\n");
  const sig = hex(hmacSha256(signingKey(c, date), utf8(sts)));
  return {
    "x-amz-date": t,
    "x-amz-content-sha256": payloadHash,
    authorization: `AWS4-HMAC-SHA256 Credential=${c.accessKey}/${scope}, SignedHeaders=${signed}, Signature=${sig}`,
  };
}

/** A presigned GET URL (for downloads and shareable links). */
export function presign(
  c: Creds,
  path: string,
  host: string,
  origin: string,
  expiresSecs: number,
  extra: [string, string][] = [],
  now = new Date(),
): string {
  const t = amzDate(now);
  const date = t.slice(0, 8);
  const scope = `${date}/${c.region}/s3/aws4_request`;
  const query: [string, string][] = [
    ...extra,
    ["X-Amz-Algorithm", "AWS4-HMAC-SHA256"],
    ["X-Amz-Credential", `${c.accessKey}/${scope}`],
    ["X-Amz-Date", t],
    ["X-Amz-Expires", String(expiresSecs)],
    ["X-Amz-SignedHeaders", "host"],
  ];
  const creq = ["GET", path, canonicalQuery(query), `host:${host}\n`, "host", UNSIGNED].join("\n");
  const sts = ["AWS4-HMAC-SHA256", t, scope, hex(sha256(utf8(creq)))].join("\n");
  const sig = hex(hmacSha256(signingKey(c, date), utf8(sts)));
  return `${origin}${path}?${canonicalQuery(query)}&X-Amz-Signature=${sig}`;
}
