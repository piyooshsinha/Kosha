import { useState } from "react";

/** A JSON policy textarea with validation, Save, and optional Remove. */
export function PolicyEditor({
  initial,
  template,
  onSave,
  onRemove,
  onCancel,
}: {
  initial: string | null;
  template: unknown;
  onSave: (doc: string) => Promise<void>;
  onRemove?: () => Promise<void>;
  onCancel: () => void;
}) {
  const pretty = (s: string) => {
    try {
      return JSON.stringify(JSON.parse(s), null, 2);
    } catch {
      return s;
    }
  };
  const [text, setText] = useState(
    initial ? pretty(initial) : JSON.stringify(template, null, 2),
  );
  const [busy, setBusy] = useState(false);
  let error = "";
  try {
    const v = JSON.parse(text);
    if (typeof v !== "object" || v === null || Array.isArray(v))
      error = "A policy is a JSON object.";
  } catch (e) {
    error = (e as Error).message;
  }
  const run = async (f: () => Promise<void>) => {
    setBusy(true);
    try {
      await f();
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="policy-editor">
      <textarea
        className="mono"
        spellCheck={false}
        value={text}
        onChange={(e) => setText(e.target.value)}
        rows={16}
        aria-label="Policy JSON"
      />
      {error && <p className="fine danger-text">{error}</p>}
      <div className="actions">
        <button
          className="primary"
          disabled={!!error || busy}
          onClick={() => run(() => onSave(text))}
        >
          Save policy
        </button>
        {onRemove && initial && (
          <button
            className="ghost danger"
            disabled={busy}
            onClick={() => run(onRemove)}
          >
            Remove policy
          </button>
        )}
        <button className="ghost" onClick={onCancel}>
          Cancel
        </button>
      </div>
    </div>
  );
}
