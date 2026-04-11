import type { ReactNode } from "react";

interface FiscalPageShellProps {
  title: string;
  subtitle?: string;
  children: ReactNode;
}

export function FiscalPageShell({
  title,
  subtitle,
  children,
}: FiscalPageShellProps) {
  return (
    <div className="h-full overflow-auto p-4 text-slate-200">
      <div className="mb-4 rounded-2xl border border-slate-700 bg-slate-900/40 p-4">
        <div className="text-lg font-semibold text-white">{title}</div>
        {subtitle ? <div className="mt-1 text-sm text-slate-400">{subtitle}</div> : null}
      </div>
      {children}
    </div>
  );
}

export default FiscalPageShell;
