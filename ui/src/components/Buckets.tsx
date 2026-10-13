import { Fragment, useCallback, useEffect, useState, type FormEvent } from "react";
import type { Bucket, S3 } from "../api/s3.ts";
import { when } from "../format.ts";
import { href } from "../App.tsx";
import { PolicyEditor } from "./PolicyEditor.tsx";
import type { PushToast } from "./Toasts.tsx";

const publicRead = (bucket: string) => ({
  Version: "2012-10-17",
  Statement: [{ Sid: "PublicRead", Effect: "Allow", Principal: "*", Action: "s3:GetObject", Resource: `arn:aws:s3:::${bucket}/*` }],
});

const NAME_RULE = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;

export function Buckets({ s3, toast }: { s3: S3; toast: PushToast }) {
  const [buckets, setBuckets] = useState<Bucket[] | null>(null);
  const [name, setName] = useState("");
  const [busy, setBusy] = useState(false);
  const [policy, setPolicy] = useState<{ bucket: string; doc: string | null } | null>(null);

  async function openPolicy(bucket: string) {
    if (policy?.bucket === bucket) return setPolicy(null);
    try {
      setPolicy({ bucket, doc: await s3.getBucketPolicy(bucket) });
    } catch (err) {
      toast("error", (err as Error).message);
    }
  }

  async function savePolicy(bucket: string, doc: string | null) {
    try {
      await s3.putBucketPolicy(bucket, doc);
      toast("ok", doc ? `Saved policy for ${bucket}` : `Removed policy from ${bucket}`);
      setPolicy(null);
    } catch (err) {
      toast("error", (err as Error).message);
    }
  }

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
              <Fragment key={b.name}>
                <tr>
                <td>
                  <a className="strong-link" href={href({ page: "browse", bucket: b.name, prefix: "" })}>
                    <span className="glyph">▤</span>
                    {b.name}
                  </a>
                </td>
                <td className="muted">{when(b.created)}</td>
                <td className="right nowrap">
                  <button className="ghost small" onClick={() => openPolicy(b.name)}>
                    Policy
                  </button>
                  <button className="ghost small danger" onClick={() => remove(b.name)}>
                    Delete
                  </button>
                </td>
              </tr>
              {policy?.bucket === b.name && (
                <tr>
                  <td colSpan={3}>
                    <p className="fine muted">
                      Bucket policy. <code>"Principal": "*"</code> grants anonymous access; users are named as{" "}
                      <code>arn:aws:iam:::user/NAME</code>.
                    </p>
                    <PolicyEditor
                      initial={policy.doc}
                      template={publicRead(b.name)}
                      onSave={(doc) => savePolicy(b.name, doc)}
                      onRemove={() => savePolicy(b.name, null)}
                      onCancel={() => setPolicy(null)}
                    />
                  </td>
                </tr>
              )}
              </Fragment>
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
