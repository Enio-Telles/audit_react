import type { DossieViewMode } from '../utils/dossie_helpers';

interface DossieViewModeToggleProps {
  mode: DossieViewMode;
  onChange: (mode: DossieViewMode) => void;
}

export function DossieViewModeToggle({ mode, onChange }: DossieViewModeToggleProps) {
  return (
    <div className="flex items-center gap-1 rounded-lg border border-slate-700 bg-slate-900/80 p-1">
      <button
        type="button"
        onClick={() => onChange('executivo')}
        className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${
          mode === 'executivo'
            ? 'border border-blue-600/60 bg-blue-700/60 text-blue-100'
            : 'text-slate-400 hover:text-slate-200'
        }`}
      >
        Executivo
      </button>
      <button
        type="button"
        onClick={() => onChange('auditoria')}
        className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${
          mode === 'auditoria'
            ? 'border border-blue-600/60 bg-blue-700/60 text-blue-100'
            : 'text-slate-400 hover:text-slate-200'
        }`}
      >
        Auditoria
      </button>
    </div>
  );
}
