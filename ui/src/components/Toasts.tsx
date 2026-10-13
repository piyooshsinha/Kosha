import { useCallback, useRef, useState } from "react";

export type ToastKind = "ok" | "error";
export type Toast = { id: number; kind: ToastKind; text: string };
export type PushToast = (kind: ToastKind, text: string) => void;

export function useToasts() {
  const [items, setItems] = useState<Toast[]>([]);
  const next = useRef(1);
  const dismiss = useCallback((id: number) => setItems((t) => t.filter((x) => x.id !== id)), []);
  const push = useCallback<PushToast>(
    (kind, text) => {
      const id = next.current++;
      setItems((t) => [...t, { id, kind, text }]);
      setTimeout(() => dismiss(id), kind === "error" ? 8000 : 4000);
    },
    [dismiss],
  );
  return { items, push, dismiss };
}

export function Toasts({ items, dismiss }: { items: Toast[]; dismiss: (id: number) => void }) {
  return (
    <div className="toasts" role="status" aria-live="polite">
      {items.map((t) => (
        <div key={t.id} className={`toast ${t.kind}`} onClick={() => dismiss(t.id)}>
          {t.text}
        </div>
      ))}
    </div>
  );
}
