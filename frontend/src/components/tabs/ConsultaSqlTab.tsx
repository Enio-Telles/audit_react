import { useState } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import { sqlApi } from '../../api/client';
import { useAppStore } from '../../store/appStore';
import { DataTable } from '../table/DataTable';
import { ColumnToggle } from '../table/ColumnToggle';

export function ConsultaSqlTab() {
  const { selectedCnpj } = useAppStore();
  const [sqlText, setSqlText] = useState('');
  const [selectedSqlId, setSelectedSqlId] = useState<string | null>(null);
  const [result, setResult] = useState<{ rows: Record<string, unknown>[]; count: number } | null>(null);
  const [error, setError] = useState('');
  const [hiddenCols, setHiddenCols] = useState<Set<string>>(new Set());

  const { data: sqlFiles = [] } = useQuery({
    queryKey: ['sqlFiles'],
    queryFn: sqlApi.listFiles,
  });

  const execMutation = useMutation({
    mutationFn: () => {
      if (!selectedSqlId) throw new Error("Nenhum arquivo SQL selecionado");
      return sqlApi.execute(selectedSqlId, selectedCnpj ?? undefined);
    },
    onSuccess: (data) => { setResult(data); setError(''); },
    onError: (err: Error) => setError(err.message),
  });

  const loadFile = async (path: string) => {
    const { content } = await sqlApi.readFile(path);
    setSqlText(content);
    setSelectedSqlId(path);
  };

  const textareaCls = 'w-full bg-slate-900 border border-slate-600 rounded px-3 py-2 text-xs text-slate-200 font-mono focus:outline-none focus:border-blue-500 resize-none';
  const btnCls = 'px-3 py-1.5 rounded text-xs font-medium cursor-pointer transition-colors';

  const resultCols = result && result.rows.length > 0 ? Object.keys(result.rows[0]) : [];

  return (
    <div className="flex flex-col h-full p-3 gap-3">
      {/* SQL files list */}
      <div className="flex gap-2 flex-wrap">
        {sqlFiles.map((f) => (
          <button
            key={f.path}
            onClick={() => loadFile(f.path)}
            className="px-2 py-1 rounded text-xs bg-slate-700 hover:bg-slate-600 text-slate-300"
          >
            {f.name}
          </button>
        ))}
      </div>

      {/* SQL editor */}
      <textarea
        className={textareaCls + ' opacity-75 cursor-not-allowed'}
        rows={8}
        value={sqlText}
        readOnly
        placeholder="Selecione um arquivo SQL acima para visualizar..."
      />

      <div className="flex gap-2 flex-wrap items-center">
        <button
          onClick={() => execMutation.mutate()}
          disabled={!selectedSqlId || execMutation.isPending}
          className={btnCls + ' bg-blue-600 hover:bg-blue-500 text-white disabled:opacity-40'}
        >
          {execMutation.isPending ? 'Executando...' : 'Executar SQL'}
        </button>
        <button onClick={() => { setSqlText(''); setSelectedSqlId(null); setResult(null); setError(''); }} className={btnCls + ' bg-slate-700 hover:bg-slate-600 text-slate-200'}>
          Limpar
        </button>
        {result && (
          <ColumnToggle
            allColumns={resultCols}
            hiddenColumns={hiddenCols}
            onChange={(col, visible) =>
              setHiddenCols((prev) => {
                const next = new Set(prev);
                if (visible) next.delete(col);
                else next.add(col);
                return next;
              })
            }
            onReset={() => setHiddenCols(new Set())}
          />
        )}
        {result && <span className="text-xs text-green-400 self-center">{result.count} registros</span>}
        {error && <span className="text-xs text-red-400 self-center">{error}</span>}
      </div>

      {result && result.rows.length > 0 && (
        <div className="flex-1 overflow-hidden border border-slate-700 rounded">
          <DataTable
            appearanceKey="consulta_sql"
            columns={resultCols}
            rows={result.rows}
            totalRows={result.count}
            hiddenColumns={hiddenCols}
          />
        </div>
      )}
    </div>
  );
}
