import { useState, type FormEvent } from "react";
import { S3, S3Error } from "../api/s3.ts";
import type { Creds } from "../api/sigv4.ts";

export function Login({ onSignIn }: { onSignIn: (c: Creds) => void }) {
  const [accessKey, setAccessKey] = useState("");
  const [secretKey, setSecretKey] = useState("");
  const [region, setRegion] = useState("us-east-1");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    const creds = { accessKey: accessKey.trim(), secretKey, region: region.trim() || "us-east-1" };
    try {
      await new S3(creds).listBuckets();
      onSignIn(creds);
    } catch (err) {
      setError(
        err instanceof S3Error && (err.code === "SignatureDoesNotMatch" || err.code === "InvalidAccessKeyId")
          ? "Those credentials were not accepted."
          : err instanceof Error
            ? err.message
            : String(err),
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="login">
      <form className="card login-card" onSubmit={submit}>
        <div className="brand big">
          <span className="logo">K</span>
          <span>Kosha</span>
        </div>
        <p className="muted">Sign in with an access key for this server.</p>
        <label>
          Access key
          <input value={accessKey} onChange={(e) => setAccessKey(e.target.value)} autoComplete="username" required autoFocus />
        </label>
        <label>
          Secret key
          <input type="password" value={secretKey} onChange={(e) => setSecretKey(e.target.value)} autoComplete="current-password" required />
        </label>
        <details>
          <summary>Advanced</summary>
          <label>
            Region
            <input value={region} onChange={(e) => setRegion(e.target.value)} />
          </label>
        </details>
        {error && <div className="error-box">{error}</div>}
        <button className="primary" disabled={busy}>
          {busy ? "Signing in…" : "Sign in"}
        </button>
        <p className="fine">Requests are signed in your browser with SigV4. The secret never leaves this tab.</p>
      </form>
    </div>
  );
}
