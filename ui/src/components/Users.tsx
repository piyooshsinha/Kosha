import {
  Fragment,
  useCallback,
  useEffect,
  useState,
  type FormEvent,
} from "react";
import type { S3, User } from "../api/s3.ts";
import { when } from "../format.ts";
import { PolicyEditor } from "./PolicyEditor.tsx";
import type { PushToast } from "./Toasts.tsx";

const READ_ONLY = {
  Version: "2012-10-17",
  Statement: [
    { Effect: "Allow", Action: ["s3:ListAllMyBuckets"], Resource: "*" },
    {
      Effect: "Allow",
      Action: ["s3:ListBucket", "s3:GetObject"],
      Resource: ["arn:aws:s3:::*"],
    },
  ],
};

const HOME_DIR = {
  Version: "2012-10-17",
  Statement: [
    {
      Effect: "Allow",
      Action: ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      Resource: "arn:aws:s3:::home/${aws:username}/*",
    },
    {
      Effect: "Allow",
      Action: "s3:ListBucket",
      Resource: "arn:aws:s3:::home",
      Condition: { StringLike: { "s3:prefix": "${aws:username}/*" } },
    },
  ],
};

export function Users({ s3, toast }: { s3: S3; toast: PushToast }) {
  const [users, setUsers] = useState<User[] | null>(null);
  const [name, setName] = useState("");
  const [template, setTemplate] = useState<"none" | "read" | "home">("read");
  const [created, setCreated] = useState<User | null>(null);
  const [editing, setEditing] = useState<string | null>(null);

  const load = useCallback(() => {
    s3.listUsers()
      .then(setUsers)
      .catch((e) =>
        toast(
          "error",
          e.code === "AccessDenied"
            ? "Managing users requires a root access key."
            : e.message,
        ),
      );
  }, [s3, toast]);
  useEffect(load, [load]);

  async function create(e: FormEvent) {
    e.preventDefault();
    try {
      const policy =
        template === "read" ? READ_ONLY : template === "home" ? HOME_DIR : null;
      setCreated(await s3.createUser(name, policy));
      setName("");
      load();
    } catch (err) {
      toast("error", (err as Error).message);
    }
  }

  async function act(f: () => Promise<unknown>, ok: string) {
    try {
      const r = await f();
      if (
        r &&
        typeof r === "object" &&
        "secret_key" in r &&
        (r as User).secret_key
      )
        setCreated(r as User);
      toast("ok", ok);
      load();
    } catch (err) {
      toast("error", (err as Error).message);
    }
  }

  return (
    <>
      <header className="page-head">
        <div>
          <h1>Users</h1>
          <p className="muted">Access keys governed by policies (root keys are not listed).</p>
        </div>
        <form className="inline-form" onSubmit={create}>
          <input
            placeholder="user-name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            aria-label="New user name"
          />
          <select
            value={template}
            onChange={(e) => setTemplate(e.target.value as typeof template)}
            aria-label="Starting policy"
          >
            <option value="read">Read-only</option>
            <option value="home">Home directory</option>
            <option value="none">No policy</option>
          </select>
          <button
            className="primary"
            disabled={!/^[A-Za-z0-9+=,.@_-]{1,64}$/.test(name)}
          >
            Create user
          </button>
        </form>
      </header>

      {created?.secret_key && (
        <div className="card secret-card">
          <h3>Keys for {created.name}</h3>
          <p className="muted">
            The secret key is shown only now. Store it somewhere safe.
          </p>
          <div className="props">
            <span>Access key</span>
            <code className="code">{created.access_key}</code>
            <span>Secret key</span>
            <code className="code">{created.secret_key}</code>
          </div>
          <button className="ghost small" onClick={() => setCreated(null)}>
            Done
          </button>
        </div>
      )}

      <div className="card flush">
        <table className="table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Access key</th>
              <th>Status</th>
              <th>Created</th>
              <th className="right" />
            </tr>
          </thead>
          <tbody>
            {users?.map((u) => (
              <Fragment key={u.access_key}>
                <tr>
                  <td className="strong">{u.name}</td>
                  <td className="mono">{u.access_key}</td>
                  <td>
                    <span
                      className={`badge ${u.enabled ? "leader" : "follower"}`}
                    >
                      {u.enabled ? "active" : "disabled"}
                    </span>
                    {!u.policy && <span className="tag">no policy</span>}
                  </td>
                  <td className="muted">{when(u.created)}</td>
                  <td className="right nowrap">
                    <button
                      className="ghost small"
                      onClick={() =>
                        setEditing(
                          editing === u.access_key ? null : u.access_key,
                        )
                      }
                    >
                      Policy
                    </button>
                    <button
                      className="ghost small"
                      onClick={() =>
                        act(
                          () =>
                            s3.updateUser(u.access_key, {
                              enabled: !u.enabled,
                            }),
                          `${u.enabled ? "Disabled" : "Enabled"} ${u.name}`,
                        )
                      }
                    >
                      {u.enabled ? "Disable" : "Enable"}
                    </button>
                    <button
                      className="ghost small"
                      onClick={() =>
                        confirm(
                          `Issue a new secret for ${u.name}? The old one stops working.`,
                        ) &&
                        act(
                          () =>
                            s3.updateUser(u.access_key, {
                              rotate_secret: true,
                            }),
                          `Rotated ${u.name}'s secret`,
                        )
                      }
                    >
                      Rotate
                    </button>
                    <button
                      className="ghost small danger"
                      onClick={() =>
                        confirm(`Delete user ${u.name}?`) &&
                        act(
                          () => s3.deleteUser(u.access_key),
                          `Deleted ${u.name}`,
                        )
                      }
                    >
                      Delete
                    </button>
                  </td>
                </tr>
                {editing === u.access_key && (
                  <tr>
                    <td colSpan={5}>
                      <PolicyEditor
                        initial={u.policy ? JSON.stringify(u.policy) : null}
                        template={READ_ONLY}
                        onSave={(doc) =>
                          act(
                            () =>
                              s3.updateUser(u.access_key, {
                                policy: JSON.parse(doc),
                              }),
                            `Updated ${u.name}'s policy`,
                          ).then(() => setEditing(null))
                        }
                        onRemove={() =>
                          act(
                            () => s3.updateUser(u.access_key, { policy: null }),
                            `Removed ${u.name}'s policy`,
                          ).then(() => setEditing(null))
                        }
                        onCancel={() => setEditing(null)}
                      />
                    </td>
                  </tr>
                )}
              </Fragment>
            ))}
            {users?.length === 0 && (
              <tr>
                <td colSpan={5} className="empty">
                  No users yet. Create one above; it gets its own access key.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </>
  );
}
