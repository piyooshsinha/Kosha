import { useCallback, useEffect, useRef, useState, type DragEvent } from "react";
import type { Listing, ObjectEntry, ObjectInfo, S3 } from "../api/s3.ts";
import { bytes, when } from "../format.ts";
import { href } from "../App.tsx";
import type { PushToast } from "./Toasts.tsx";

type Upload = { name: string; sent: number; total: number; state: "running" | "done" | "failed" };

const plural = (n: number, word: string) => `${n} ${word}${n === 1 ? "" : "s"}`;

const EXPIRY = [
  { label: "1 hour", secs: 3600 },
  { label: "1 day", secs: 86400 },
  { label: "7 days", secs: 604800 },
];

export function Browser({ s3, bucket, prefix, toast }: { s3: S3; bucket: string; prefix: string; toast: PushToast }) {
  const [listing, setListing] = useState<Listing | null>(null);
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [uploads, setUploads] = useState<Upload[]>([]);
  const [dragging, setDragging] = useState(false);
  const [detail, setDetail] = useState<{ key: string; info?: ObjectInfo } | null>(null);
  const fileInput = useRef<HTMLInputElement>(null);

  const load = useCallback(
    async (token?: string) => {
      setLoading(true);
      try {
        const page = await s3.list(bucket, prefix, token);
        setListing((prev) =>
          token && prev
            ? { objects: [...prev.objects, ...page.objects], prefixes: [...prev.prefixes, ...page.prefixes], nextToken: page.nextToken }
            : page,
        );
      } catch (e) {
        toast("error", (e as Error).message);
      } finally {
        setLoading(false);
      }
    },
    [s3, bucket, prefix, toast],
  );

  useEffect(() => {
    setListing(null);
    setSelected(new Set());
    setDetail(null);
    load();
  }, [load]);

  async function uploadFiles(files: FileList | File[]) {
    const list = Array.from(files);
    if (!list.length) return;
    const base = uploads.length;
    setUploads((u) => [...u, ...list.map((f) => ({ name: f.name, sent: 0, total: f.size, state: "running" as const }))]);
    // A few uploads at a time keeps the browser responsive on large drops.
    let next = 0;
    const worker = async () => {
      while (next < list.length) {
        const i = next++;
        const f = list[i];
        const update = (patch: Partial<Upload>) =>
          setUploads((u) => u.map((x, j) => (j === base + i ? { ...x, ...patch } : x)));
        try {
          await s3.upload(bucket, prefix + f.name, f, (sent, total) => update({ sent, total }));
          update({ state: "done", sent: f.size });
        } catch (e) {
          update({ state: "failed" });
          toast("error", `${f.name}: ${(e as Error).message}`);
        }
      }
    };
    await Promise.all([worker(), worker(), worker()]);
    toast("ok", `Uploaded ${list.length} file${list.length === 1 ? "" : "s"}`);
    load();
    setTimeout(() => setUploads((u) => u.filter((x) => x.state !== "done")), 3000);
  }

  function onDrop(e: DragEvent) {
    e.preventDefault();
    setDragging(false);
    uploadFiles(e.dataTransfer.files);
  }

  async function newFolder() {
    const name = prompt("Folder name")?.trim().replace(/\/+$/, "");
    if (!name) return;
    try {
      await s3.createFolder(bucket, `${prefix}${name}/`);
      load();
    } catch (e) {
      toast("error", (e as Error).message);
    }
  }

  async function deleteSelected() {
    const keys = Array.from(selected);
    if (!keys.length || !confirm(`Delete ${keys.length} object${keys.length === 1 ? "" : "s"}?`)) return;
    try {
      await s3.deleteObjects(bucket, keys);
      toast("ok", `Deleted ${keys.length} object${keys.length === 1 ? "" : "s"}`);
      setSelected(new Set());
      setDetail(null);
      load();
    } catch (e) {
      toast("error", (e as Error).message);
    }
  }

  async function openDetail(o: ObjectEntry) {
    setDetail({ key: o.key });
    try {
      const info = await s3.head(bucket, o.key);
      setDetail({ key: o.key, info });
    } catch (e) {
      toast("error", (e as Error).message);
    }
  }

  function toggle(key: string) {
    setSelected((s) => {
      const n = new Set(s);
      if (n.has(key)) n.delete(key);
      else n.add(key);
      return n;
    });
  }

  const crumbs = prefix.split("/").filter(Boolean);
  const objects = listing?.objects.filter((o) => o.key !== prefix) ?? [];
  const allSelected = objects.length > 0 && objects.every((o) => selected.has(o.key));

  return (
    <div className="browser">
      <header className="page-head">
        <div>
          <nav className="crumbs" aria-label="Path">
            <a href={href({ page: "buckets" })}>Buckets</a>
            <span>/</span>
            <a href={href({ page: "browse", bucket, prefix: "" })}>{bucket}</a>
            {crumbs.map((c, i) => (
              <span key={i} className="crumb">
                <span>/</span>
                <a href={href({ page: "browse", bucket, prefix: crumbs.slice(0, i + 1).join("/") + "/" })}>{c}</a>
              </span>
            ))}
          </nav>
          <p className="muted">
            {listing ? `${plural(listing.prefixes.length, "folder")} · ${plural(objects.length, "object")}${listing.nextToken ? " (more below)" : ""}` : "Loading…"}
          </p>
        </div>
        <div className="actions">
          {selected.size > 0 && (
            <button className="danger" onClick={deleteSelected}>
              Delete {selected.size}
            </button>
          )}
          <button onClick={newFolder}>New folder</button>
          <button className="primary" onClick={() => fileInput.current?.click()}>
            Upload
          </button>
          <input
            ref={fileInput}
            type="file"
            multiple
            hidden
            onChange={(e) => {
              if (e.target.files) uploadFiles(e.target.files);
              e.target.value = "";
            }}
          />
        </div>
      </header>

      {uploads.length > 0 && (
        <div className="card uploads">
          {uploads.map((u, i) => (
            <div key={i} className={`upload ${u.state}`}>
              <span className="upload-name">{u.name}</span>
              <div className="bar-track thin">
                <div className="bar-fill tone-a" style={{ width: `${u.total ? (u.sent / u.total) * 100 : 100}%` }} />
              </div>
              <span className="muted">{u.state === "failed" ? "failed" : `${bytes(u.sent)} / ${bytes(u.total)}`}</span>
            </div>
          ))}
        </div>
      )}

      <div className="split">
        <div
          className={`card flush dropzone${dragging ? " dragging" : ""}`}
          onDragOver={(e) => {
            e.preventDefault();
            setDragging(true);
          }}
          onDragLeave={() => setDragging(false)}
          onDrop={onDrop}
        >
          <table className="table">
            <thead>
              <tr>
                <th className="check">
                  <input
                    type="checkbox"
                    aria-label="Select all"
                    checked={allSelected}
                    onChange={() => setSelected(allSelected ? new Set() : new Set(objects.map((o) => o.key)))}
                  />
                </th>
                <th>Name</th>
                <th className="right">Size</th>
                <th>Modified</th>
              </tr>
            </thead>
            <tbody>
              {listing?.prefixes.map((p) => (
                <tr key={p}>
                  <td className="check" />
                  <td colSpan={3}>
                    <a className="strong-link" href={href({ page: "browse", bucket, prefix: p })}>
                      <span className="glyph folder">▸</span>
                      {p.slice(prefix.length)}
                    </a>
                  </td>
                </tr>
              ))}
              {objects.map((o) => (
                <tr key={o.key} className={detail?.key === o.key ? "selected" : ""}>
                  <td className="check">
                    <input type="checkbox" aria-label={`Select ${o.key}`} checked={selected.has(o.key)} onChange={() => toggle(o.key)} />
                  </td>
                  <td>
                    <button className="link" onClick={() => openDetail(o)}>
                      <span className="glyph">▫</span>
                      {o.key.slice(prefix.length)}
                    </button>
                  </td>
                  <td className="right mono">{bytes(o.size)}</td>
                  <td className="muted">{when(o.lastModified)}</td>
                </tr>
              ))}
              {listing && !listing.prefixes.length && !objects.length && (
                <tr>
                  <td colSpan={4} className="empty">
                    This folder is empty. Drop files here or use Upload.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
          {listing?.nextToken && (
            <div className="more">
              <button disabled={loading} onClick={() => load(listing.nextToken)}>
                {loading ? "Loading…" : "Load more"}
              </button>
            </div>
          )}
          {dragging && <div className="drop-hint">Drop to upload to /{prefix}</div>}
        </div>

        {detail && <Detail s3={s3} bucket={bucket} detail={detail} onClose={() => setDetail(null)} toast={toast} />}
      </div>
    </div>
  );
}

function Detail({
  s3,
  bucket,
  detail,
  onClose,
  toast,
}: {
  s3: S3;
  bucket: string;
  detail: { key: string; info?: ObjectInfo };
  onClose: () => void;
  toast: PushToast;
}) {
  const [expiry, setExpiry] = useState(EXPIRY[0].secs);
  const info = detail.info;

  async function copyLink() {
    const url = s3.shareLink(bucket, detail.key, expiry);
    try {
      await navigator.clipboard.writeText(url);
      toast("ok", "Share link copied");
    } catch {
      prompt("Share link", url);
    }
  }

  return (
    <aside className="card detail">
      <div className="detail-head">
        <h2 title={detail.key}>{detail.key.split("/").pop()}</h2>
        <button className="ghost small" onClick={onClose} aria-label="Close">
          ✕
        </button>
      </div>
      {!info ? (
        <p className="muted">Loading…</p>
      ) : (
        <>
          <dl className="props">
            <dt>Key</dt>
            <dd className="mono wrap">{detail.key}</dd>
            <dt>Size</dt>
            <dd>
              {bytes(info.size)} <span className="muted">({info.size.toLocaleString()} bytes)</span>
            </dd>
            <dt>Type</dt>
            <dd>{info.contentType}</dd>
            <dt>Modified</dt>
            <dd>{info.lastModified}</dd>
            <dt>ETag</dt>
            <dd className="mono wrap">{info.etag}</dd>
            {Object.entries(info.metadata).map(([k, v]) => (
              <span key={k} className="contents">
                <dt>meta: {k}</dt>
                <dd className="wrap">{v}</dd>
              </span>
            ))}
          </dl>
          <div className="detail-actions">
            <a className="button primary" href={s3.shareLink(bucket, detail.key, 300, true)}>
              Download
            </a>
            <a className="button" href={s3.shareLink(bucket, detail.key, 300)} target="_blank" rel="noreferrer">
              Open
            </a>
          </div>
          <div className="share">
            <label>
              Share link valid for
              <select value={expiry} onChange={(e) => setExpiry(Number(e.target.value))}>
                {EXPIRY.map((x) => (
                  <option key={x.secs} value={x.secs}>
                    {x.label}
                  </option>
                ))}
              </select>
            </label>
            <button onClick={copyLink}>Copy link</button>
          </div>
        </>
      )}
    </aside>
  );
}
