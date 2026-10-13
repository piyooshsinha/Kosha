// Run with `npm test` (Node's built-in test runner).
import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash, createHmac } from "node:crypto";
import { hex, hmacSha256, sha256, utf8 } from "./sha256.ts";
import { signHeaders } from "./sigv4.ts";

test("sha256 matches node:crypto across block boundaries", () => {
  for (const n of [0, 1, 55, 56, 63, 64, 65, 119, 120, 1000, 100_000]) {
    const data = new Uint8Array(n).map((_, i) => (i * 31 + 7) & 0xff);
    assert.equal(hex(sha256(data)), createHash("sha256").update(data).digest("hex"), `len ${n}`);
  }
});

test("hmac matches node:crypto, including long keys", () => {
  for (const keyLen of [0, 10, 64, 65, 200]) {
    const key = new Uint8Array(keyLen).fill(0x42);
    const msg = utf8("kosha console");
    assert.equal(hex(hmacSha256(key, msg)), createHmac("sha256", key).update(msg).digest("hex"));
  }
});

test("matches the AWS SigV4 documentation example", () => {
  // GET /test.txt with Range from the S3 SigV4 docs, minus the range header.
  // Recomputed independently with node:crypto to cross-check the chain.
  const creds = { accessKey: "AKIAIOSFODNN7EXAMPLE", secretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", region: "us-east-1" };
  const now = new Date(Date.UTC(2013, 4, 24));
  const h = signHeaders(creds, "GET", "/test.txt", [], "examplebucket.s3.amazonaws.com", "UNSIGNED-PAYLOAD", now);
  const creq = "GET\n/test.txt\n\nhost:examplebucket.s3.amazonaws.com\nx-amz-content-sha256:UNSIGNED-PAYLOAD\nx-amz-date:20130524T000000Z\n\nhost;x-amz-content-sha256;x-amz-date\nUNSIGNED-PAYLOAD";
  const sts = `AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n${createHash("sha256").update(creq).digest("hex")}`;
  let k: Buffer = createHmac("sha256", "AWS4" + creds.secretKey).update("20130524").digest();
  for (const p of ["us-east-1", "s3", "aws4_request"]) k = createHmac("sha256", k).update(p).digest();
  const sig = createHmac("sha256", k).update(sts).digest("hex");
  assert.match(h.authorization, new RegExp(`Signature=${sig}$`));
});
