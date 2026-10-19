// A small S3 client for the console, speaking to the server that served it.
import { awsEncode, presign, signHeaders, type Creds } from "./sigv4.ts";

export interface Bucket {
  name: string;
  created: string;
}

export interface ObjectEntry {
  key: string;
  size: number;
  etag: string;
  lastModified: string;
}

export interface Listing {
  objects: ObjectEntry[];
  prefixes: string[];
  nextToken?: string;
}

export interface ObjectInfo {
  size: number;
  etag: string;
  contentType: string;
  lastModified: string;
  metadata: Record<string, string>;
}

export interface Stats {
  io_backend: string;
  buckets: number;
  objects: number;
  logical_bytes: number;
  unique_bytes: number;
  physical_bytes: number;
  chunks: number;
  /** Bytes and chunks held by the cold tier (absent on older servers). */
  cold_bytes?: number;
  cold_chunks?: number;
  dedup_ratio: number;
  compression_ratio: number;
  overall_ratio: number;
}

export interface ClusterNode {
  id: number;
  addr: string;
  api: string;
  voter: boolean;
  matched: number | null;
}

export interface ClusterStatus {
  mode?: "single-node";
  node_id?: number;
  state?: string;
  term?: number;
  leader?: number | null;
  last_log_index?: number | null;
  last_applied?: number | null;
  snapshot_index?: number | null;
  replication_factor?: number;
  nodes?: ClusterNode[];
  lease_reads?: number;
  quorum_reads?: number;
  repair?: {
    rounds: number;
    scanned: number;
    repaired: number;
    surplus_removed: number;
    failed: number;
    last_round_ms: number;
  };
}

/** Where one chunk of a write was stored. */
export interface ChunkTrace {
  id: string;
  len: number;
  stored: number;
  replicas: number[];
  failed: number[];
  shards: [number, number][];
  layout: string | null;
  start_ms: number;
  end_ms: number;
}

/** One write, as recorded by the node that received it. */
export interface Flow {
  node: number;
  seq: number;
  op: string;
  bucket: string;
  key: string;
  size: number;
  inline_bytes: number;
  chunk_count: number;
  chunks: ChunkTrace[];
  reused_chunks: number;
  leader: number | null;
  forwarded: boolean;
  log_index: number | null;
  batch: number;
  started_ms: number;
  proposed_ms: number;
  committed_ms: number;
  error: string | null;
}

export interface FlowsView {
  mode?: "single-node";
  node_id: number;
  leader: number | null;
  term: number;
  replication_factor: number;
  erasure: string | null;
  nodes: { id: number; api: string; voter: boolean; reachable: boolean; last_applied: number | null }[];
  flows: Flow[];
}

export interface User {
  access_key: string;
  name: string;
  policy: unknown;
  enabled: boolean;
  created: string;
  /** Only present right after creation or rotation. */
  secret_key?: string;
}

export class S3Error extends Error {
  constructor(
    public status: number,
    public code: string,
    message: string,
  ) {
    super(message);
  }
}

const text = (el: Element, tag: string) => el.getElementsByTagName(tag)[0]?.textContent ?? "";

export function objectPath(bucket: string, key = ""): string {
  return `/${awsEncode(bucket)}/${awsEncode(key, true)}`;
}

export class S3 {
  host = location.host;
  origin = location.origin;

  constructor(public creds: Creds) {}

  private async request(
    method: string,
    path: string,
    query: [string, string][] = [],
    body?: BodyInit | null,
    headers: Record<string, string> = {},
  ): Promise<Response> {
    const signed = signHeaders(this.creds, method, path, query, this.host);
    const qs = query.map(([k, v]) => `${awsEncode(k)}=${awsEncode(v)}`).join("&");
    const resp = await fetch(`${path}${qs ? "?" + qs : ""}`, {
      method,
      body,
      headers: { ...headers, ...signed },
    });
    if (!resp.ok) {
      const raw = method === "HEAD" ? "" : await resp.text();
      const doc = raw ? new DOMParser().parseFromString(raw, "application/xml") : null;
      const code = doc ? text(doc.documentElement, "Code") : "";
      const msg = doc ? text(doc.documentElement, "Message") : "";
      throw new S3Error(resp.status, code || String(resp.status), msg || resp.statusText || "Request failed");
    }
    return resp;
  }

  private async xml(method: string, path: string, query: [string, string][] = [], body?: string) {
    const resp = await this.request(method, path, query, body);
    return new DOMParser().parseFromString(await resp.text(), "application/xml").documentElement;
  }

  async listBuckets(): Promise<Bucket[]> {
    const doc = await this.xml("GET", "/");
    return Array.from(doc.getElementsByTagName("Bucket")).map((b) => ({
      name: text(b, "Name"),
      created: text(b, "CreationDate"),
    }));
  }

  async createBucket(name: string) {
    await this.request("PUT", `/${awsEncode(name)}`);
  }

  async deleteBucket(name: string) {
    await this.request("DELETE", `/${awsEncode(name)}`);
  }

  async list(bucket: string, prefix: string, token?: string, maxKeys = 200): Promise<Listing> {
    const q: [string, string][] = [
      ["list-type", "2"],
      ["delimiter", "/"],
      ["prefix", prefix],
      ["max-keys", String(maxKeys)],
    ];
    if (token) q.push(["continuation-token", token]);
    const doc = await this.xml("GET", `/${awsEncode(bucket)}`, q);
    return {
      objects: Array.from(doc.getElementsByTagName("Contents")).map((c) => ({
        key: text(c, "Key"),
        size: Number(text(c, "Size")),
        etag: text(c, "ETag").replace(/"/g, ""),
        lastModified: text(c, "LastModified"),
      })),
      prefixes: Array.from(doc.getElementsByTagName("CommonPrefixes")).map((p) => text(p, "Prefix")),
      nextToken: text(doc, "IsTruncated") === "true" ? text(doc, "NextContinuationToken") : undefined,
    };
  }

  async head(bucket: string, key: string): Promise<ObjectInfo> {
    const r = await this.request("HEAD", objectPath(bucket, key));
    const metadata: Record<string, string> = {};
    r.headers.forEach((v, k) => {
      if (k.startsWith("x-amz-meta-")) metadata[k.slice(11)] = v;
    });
    return {
      size: Number(r.headers.get("content-length") ?? 0),
      etag: (r.headers.get("etag") ?? "").replace(/"/g, ""),
      contentType: r.headers.get("content-type") ?? "",
      lastModified: r.headers.get("last-modified") ?? "",
      metadata,
    };
  }

  async deleteObjects(bucket: string, keys: string[]) {
    const esc = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    for (let i = 0; i < keys.length; i += 1000) {
      const body =
        "<Delete><Quiet>true</Quiet>" +
        keys
          .slice(i, i + 1000)
          .map((k) => `<Object><Key>${esc(k)}</Key></Object>`)
          .join("") +
        "</Delete>";
      await this.request("POST", `/${awsEncode(bucket)}`, [["delete", ""]], body, {
        "content-type": "application/xml",
      });
    }
  }

  /** Upload with progress (fetch cannot report upload progress). */
  upload(bucket: string, key: string, body: Blob, onProgress?: (sent: number, total: number) => void): Promise<void> {
    const path = objectPath(bucket, key);
    const signed = signHeaders(this.creds, "PUT", path, [], this.host);
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open("PUT", path);
      for (const [k, v] of Object.entries(signed)) xhr.setRequestHeader(k, v);
      xhr.setRequestHeader("content-type", body.type || "application/octet-stream");
      xhr.upload.onprogress = (e) => onProgress?.(e.loaded, e.total);
      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) return resolve();
        const doc = new DOMParser().parseFromString(xhr.responseText || "<x/>", "application/xml");
        reject(new S3Error(xhr.status, text(doc.documentElement, "Code"), text(doc.documentElement, "Message") || "Upload failed"));
      };
      xhr.onerror = () => reject(new S3Error(0, "NetworkError", "Network error during upload"));
      xhr.send(body);
    });
  }

  async createFolder(bucket: string, prefix: string) {
    await this.request("PUT", objectPath(bucket, prefix), [], new Blob([]));
  }

  /** A time-limited link anyone can use to download the object. */
  shareLink(bucket: string, key: string, expiresSecs = 3600, attachment = false): string {
    const extra: [string, string][] = attachment
      ? [["response-content-disposition", `attachment; filename="${key.split("/").pop()}"`]]
      : [];
    return presign(this.creds, objectPath(bucket, key), this.host, this.origin, expiresSecs, extra);
  }

  async stats(): Promise<Stats> {
    return (await this.request("GET", "/_kosha/stats")).json();
  }

  async removeNode(id: number) {
    await this.request("DELETE", `/_kosha/cluster/nodes/${id}`);
  }

  private json(method: string, path: string, body?: unknown) {
    return this.request(method, path, [], body === undefined ? undefined : JSON.stringify(body), {
      "content-type": "application/json",
    });
  }

  async listUsers(): Promise<User[]> {
    return (await (await this.json("GET", "/_kosha/admin/users")).json()).users;
  }

  async createUser(name: string, policy: unknown): Promise<User> {
    return (await this.json("POST", "/_kosha/admin/users", { name, policy })).json();
  }

  async updateUser(
    accessKey: string,
    change: { policy?: unknown; enabled?: boolean; rotate_secret?: boolean },
  ): Promise<User> {
    return (await this.json("PUT", `/_kosha/admin/users/${awsEncode(accessKey)}`, change)).json();
  }

  async deleteUser(accessKey: string) {
    await this.request("DELETE", `/_kosha/admin/users/${awsEncode(accessKey)}`);
  }

  /** The bucket policy as JSON text, or null when none is set. */
  async getBucketPolicy(bucket: string): Promise<string | null> {
    try {
      return await (await this.request("GET", `/${awsEncode(bucket)}`, [["policy", ""]])).text();
    } catch (e) {
      if (e instanceof S3Error && e.code === "NoSuchBucketPolicy") return null;
      throw e;
    }
  }

  async putBucketPolicy(bucket: string, policy: string | null) {
    if (policy === null) {
      await this.request("DELETE", `/${awsEncode(bucket)}`, [["policy", ""]]);
    } else {
      await this.request("PUT", `/${awsEncode(bucket)}`, [["policy", ""]], policy, {
        "content-type": "application/json",
      });
    }
  }

  async cluster(): Promise<ClusterStatus> {
    return (await this.request("GET", "/_kosha/cluster")).json();
  }

  /** Recent writes across the cluster; `limit` 0 returns only the members' reachability. */
  async flows(limit = 200): Promise<FlowsView> {
    return (await this.request("GET", "/_kosha/cluster/flows", [["limit", String(limit)]])).json();
  }
}
