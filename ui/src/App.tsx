import { useCallback, useEffect, useMemo, useState } from "react";
import { S3 } from "./api/s3.ts";
import type { Creds } from "./api/sigv4.ts";
import { Login } from "./components/Login.tsx";
import { Overview } from "./components/Overview.tsx";
import { Buckets } from "./components/Buckets.tsx";
import { Browser } from "./components/Browser.tsx";
import { ClusterView } from "./components/Cluster.tsx";
import { Toasts, useToasts } from "./components/Toasts.tsx";

const SESSION_KEY = "kosha.session";

export type Route =
  | { page: "overview" }
  | { page: "buckets" }
  | { page: "browse"; bucket: string; prefix: string }
  | { page: "cluster" };

function parseRoute(hash: string): Route {
  const parts = hash.replace(/^#\/?/, "").split("/");
  switch (parts[0]) {
    case "buckets":
      return { page: "buckets" };
    case "cluster":
      return { page: "cluster" };
    case "b":
      if (parts[1]) {
        return {
          page: "browse",
          bucket: decodeURIComponent(parts[1]),
          prefix: decodeURIComponent(parts.slice(2).join("/")),
        };
      }
      return { page: "buckets" };
    default:
      return { page: "overview" };
  }
}

export function href(r: Route): string {
  switch (r.page) {
    case "overview":
      return "#/";
    case "buckets":
      return "#/buckets";
    case "cluster":
      return "#/cluster";
    case "browse":
      return `#/b/${encodeURIComponent(r.bucket)}/${r.prefix.split("/").map(encodeURIComponent).join("/")}`;
  }
}

function loadSession(): Creds | null {
  try {
    const raw = sessionStorage.getItem(SESSION_KEY);
    return raw ? (JSON.parse(raw) as Creds) : null;
  } catch {
    return null;
  }
}

export function App() {
  const [creds, setCreds] = useState<Creds | null>(loadSession);
  const [route, setRoute] = useState<Route>(() => parseRoute(location.hash));
  const toasts = useToasts();

  useEffect(() => {
    const on = () => setRoute(parseRoute(location.hash));
    window.addEventListener("hashchange", on);
    return () => window.removeEventListener("hashchange", on);
  }, []);

  const s3 = useMemo(() => (creds ? new S3(creds) : null), [creds]);

  const signIn = useCallback((c: Creds) => {
    try {
      sessionStorage.setItem(SESSION_KEY, JSON.stringify(c));
    } catch {
      /* private mode: session lasts until reload */
    }
    setCreds(c);
  }, []);

  const signOut = useCallback(() => {
    try {
      sessionStorage.removeItem(SESSION_KEY);
    } catch {
      /* ignore */
    }
    setCreds(null);
  }, []);

  if (!s3 || !creds) return <Login onSignIn={signIn} />;

  const nav: { label: string; route: Route; active: boolean; icon: string }[] = [
    { label: "Overview", route: { page: "overview" }, active: route.page === "overview", icon: "◎" },
    { label: "Buckets", route: { page: "buckets" }, active: route.page === "buckets" || route.page === "browse", icon: "▤" },
    { label: "Cluster", route: { page: "cluster" }, active: route.page === "cluster", icon: "⬡" },
  ];

  return (
    <div className="shell">
      <aside className="sidebar">
        <a className="brand" href="#/">
          <span className="logo">K</span>
          <span>
            Kosha <small>console</small>
          </span>
        </a>
        <nav>
          {nav.map((n) => (
            <a key={n.label} href={href(n.route)} className={n.active ? "active" : ""}>
              <span className="icon" aria-hidden>
                {n.icon}
              </span>
              {n.label}
            </a>
          ))}
        </nav>
        <div className="sidebar-foot">
          <div className="who" title={creds.accessKey}>
            {creds.accessKey}
          </div>
          <button className="ghost small" onClick={signOut}>
            Sign out
          </button>
        </div>
      </aside>
      <main className="content">
        {route.page === "overview" && <Overview s3={s3} />}
        {route.page === "buckets" && <Buckets s3={s3} toast={toasts.push} />}
        {route.page === "browse" && <Browser s3={s3} bucket={route.bucket} prefix={route.prefix} toast={toasts.push} />}
        {route.page === "cluster" && <ClusterView s3={s3} toast={toasts.push} />}
      </main>
      <Toasts items={toasts.items} dismiss={toasts.dismiss} />
    </div>
  );
}
