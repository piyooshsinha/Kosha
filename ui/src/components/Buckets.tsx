import { useCallback, useEffect, useState, type FormEvent } from "react";
import type { Bucket, S3 } from "../api/s3.ts";
import { when } from "../format.ts";
import { href } from "../App.tsx";
import type { PushToast } from "./Toasts.tsx";

const NAME_RULE = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;

export function Buckets({ s3, toast }: { s3: S3; toast: PushToast }) {
  const [buckets, setBuckets] = useState<Bucket[] | null>(null);
  const [name, setName] = useState("");
  const [busy, setBusy] = useState(false);

  const load = useCallback(() => {
    s3.listBuckets()
      .then(setBuckets)
      .catch((e) => toast("error", e.message));
  }, [s3, toast]);

  useEffect(load, [load]);

  async function create(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    try {
      await s3.createBucket(name);
      toast("ok", `Created bucket ${name}`);
      setName("");
      load();
    } catch (err) {
      toast("error", (err as Error).message);
    } finally {
      setBusy(false);
    }
  }

  async function remove(b: string) {
    if (!confirm(`Delete bucket "${b}"? It must be empty.`)) return;
    try {
      await s3.deleteBucket(b);
      toast("ok", `Deleted bucket ${b}`);
      load();
    } catch (err) {
      toast("error", (err as Error).message);
    }
  }

  const valid = NAME_RULE.test(name) && !name.includes("..");

  return (
    <>
      <header className="page-head">
        <div>
          <h1>Buckets</h1>
          <p className="muted">{buckets ? `${buckets.length} bucket${buckets.length === 1 ? "" : "s"}` : "Loading…"}</p>
        </div>
        <form className="inline-form" onSubmit={create}>
          <input
            placeholder="new-bucket-name"
            value={name}
            onChange={(e) => setName(e.target.value.toLowerCase())}
            aria-label="New bucket name"
          />
          <button className="primary" disabled={!valid || busy}>
            Create bucket
          </button>
        </form>
      </header>

      <div className="card flush">
        <table className="table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Created</th>
              <th className="right" />
            </tr>
          </thead>
          <tbody>
            {buckets?.map((b) => (
              <tr key={b.name}>
                <td>
                  <a className="strong-link" href={href({ page: "browse", bucket: b.name, prefix: "" })}>
                    <span className="glyph">▤</span>
                    {b.name}
                  </a>
                </td>
                <td className="muted">{when(b.created)}</td>
                <td className="right">
                  <button className="ghost small danger" onClick={() => remove(b.name)}>
                    Delete
                  </button>
                </td>
              </tr>
            ))}
            {buckets?.length === 0 && (
              <tr>
                <td colSpan={3} className="empty">
                  No buckets yet. Create one above, or with <code>aws s3 mb s3://name</code>.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </>
  );
}
