import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { parquetApi, cnpjApi } from '../../api/client';
import { useAppStore } from '../../store/appStore';
import { DataTable } from '../table/DataTable';
import { FilterBar } from '../table/FilterBar';

export function ConsultaTab() {
  const {
    selectedFile,
    selectedCnpj,
    consultaFilters,
    addConsultaFilter,
    removeConsultaFilter,
    clearConsultaFilters,
    consultaVisibleCols,
    setConsultaVisibleCols,
    consultaPage,
    setConsultaPage,
  } = useAppStore();

  const [search, setSearch] = useState('');

  const { data: schema } = useQuery({
    queryKey: ['schema', selectedCnpj, selectedFile?.path],
    queryFn: () => cnpjApi.getSchema(selectedCnpj!, selectedFile!.path),
    enabled: !!selectedCnpj && !!selectedFile,
  });

  const allCols = schema?.columns ?? [];
  const visibleCols = consultaVisibleCols.length > 0 ? consultaVisibleCols : allCols;

  const { data, isLoading } = useQuery({
    queryKey: ['parquet', selectedFile?.path, consultaFilters, visibleCols, consultaPage],
    queryFn: () =>
      parquetApi.query({
        path: selectedFile!.path,
        filters: consultaFilters,
        visible_columns: visibleCols,
        page: consultaPage,
        page_size: 200,
      }),
    enabled: !!selectedFile,
    placeholderData: (prev) => prev,
  });

  const inputCls = 'bg-slate-800 border border-slate-600 rounded px-2 py-1 text-xs text-slate-200 focus:outline-none focus:border-blue-500';
  const btnCls = 'px-3 py-1.5 rounded text-xs font-medium cursor-pointer transition-colors';

  if (!selectedFile) {
    return (
      <div className="flex items-center justify-center h-full text-slate-500">
        Selecione um arquivo Parquet na barra lateral.
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full p-3 gap-2">
      {/* File info */}
      <div className="text-xs text-slate-400">
        {selectedFile.name} | Colunas visíveis: {visibleCols.length}/{allCols.length}
      </div>

      {/* Filter bar */}
      <FilterBar
        columns={allCols}
        filters={consultaFilters}
        onAdd={addConsultaFilter}
        onRemove={removeConsultaFilter}
        onClear={clearConsultaFilters}
      />

      {/* Toolbar */}
      <div className="flex gap-2 items-center flex-wrap">
        <input
          className={inputCls + ' w-48'}
          placeholder="Filtrar Desc. Norm"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
        <button
          className={btnCls + ' bg-slate-700 hover:bg-slate-600 text-slate-200'}
          onClick={() => {
            // Export visible columns
            if (!data) return;
            const csv = [data.columns.join(','), ...data.rows.map(r => data.columns.map(c => JSON.stringify(r[c] ?? '')).join(','))].join('\n');
            const blob = new Blob([csv], { type: 'text/csv' });
            const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'export.csv'; a.click();
          }}
        >
          Exportar CSV
        </button>
        {/* Column selector */}
        <select
          multiple
          className={inputCls + ' max-h-20'}
          style={{ display: 'none' }}
        />
        <button
          className={btnCls + ' bg-slate-700 hover:bg-slate-600 text-slate-200'}
          onClick={() => setConsultaVisibleCols([])}
        >
          Padrão
        </button>
      </div>

      {/* Table */}
      <div className="flex-1 overflow-hidden border border-slate-700 rounded">
        <DataTable
          columns={data?.columns ?? visibleCols}
          rows={data?.rows ?? []}
          totalRows={data?.total_rows}
          loading={isLoading}
          page={consultaPage}
          totalPages={data?.total_pages}
          onPageChange={setConsultaPage}
        />
      </div>
    </div>
  );
}
