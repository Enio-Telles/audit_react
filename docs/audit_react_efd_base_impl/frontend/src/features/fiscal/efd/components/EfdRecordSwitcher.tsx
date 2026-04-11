import React from "react";

type Props = {
  records: Array<{ record: string; title: string }>;
  value: string;
  onChange: (value: string) => void;
};

export function EfdRecordSwitcher({ records, value, onChange }: Props) {
  return (
    <div className="flex flex-wrap gap-2">
      {records.map((item) => {
        const active = item.record === value;
        return (
          <button
            key={item.record}
            className={`rounded border px-3 py-1 text-sm ${active ? "bg-slate-900 text-white" : "bg-white"}`}
            onClick={() => onChange(item.record)}
            type="button"
          >
            {item.title}
          </button>
        );
      })}
    </div>
  );
}
