import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { estoqueApi } from '../../api/client';
import { useAppStore } from '../../store/appStore';
import { DataTable } from '../table/DataTable';

type SubTab = 'mov_estoque' | 'tabela_mensal' | 'tabela_anual' | 'resumo' | 'produtos' | 'id_agrupados';

function EstoqueSubTab({ cnpj, sub }: { cnpj: string; sub: SubTab }) {
  const [page, setPage] = useState(1);
  const [search, setSearch] = useState('');

  const queryFn = {
    mov_estoque: () => estoqueApi.movEstoque(cnpj, page),
    tabela_mensal: () => estoqueApi.tabelaMensal(cnpj, page),
    tabela_anual: () => estoqueApi.tabelaAnual(cnpj, page),
    resumo: () => estoqueApi.tabelaAnual(cnpj, page),
    produtos: () => estoqueApi.idAgrupados(cnpj, page),
    id_agrupados: () => estoqueApi.idAgrupados(cnpj, page),
  }[sub];

  const { data, isLoading } = useQuery({
    queryKey: [sub, cnpj, page],
    queryFn,
    placeholderData: (prev) => prev,
  });

  const inputCls = 'bg-slate-800 border border-slate-600 rounded px-2 py-1 text-xs text-slate-200 focus:outline-none focus:border-blue-500';
  const btnCls = 'px-3 py-1.5 rounded text-xs font-medium cursor-pointer transition-colors';

  const rows = (data?.rows ?? []).filter((r) => {
    if (!search) return true;
    return Object.values(r).some((v) => String(v ?? '').toLowerCase().includes(search.toLowerCase()));
  });

  const tableTitle = {
    mov_estoque: 'Tabela: mov_estoque',
    tabela_mensal: 'Tabela: aba_mensal',
    tabela_anual: 'Tabela: aba_anual',
    resumo: 'Resumo Global',
    produtos: 'Tabela: produtos_selecionados',
    id_agrupados: 'Tabela: id_agrupados',
  }[sub];

  return (
    <div className="flex flex-col h-full">
      <div className="px-3 py-2 border-b border-slate-700">
        <div className="text-xs font-semibold text-slate-300 mb-2">{tableTitle}</div>
        <div className="flex gap-2 items-center flex-wrap">
          <button className={btnCls + ' bg-slate-700 hover:bg-slate-600 text-slate-200'}>Recarregar</button>
          <button className={btnCls + ' bg-slate-700 hover:bg-slate-600 text-slate-200'}>Aplicar filtros</button>
          <button className={btnCls + ' bg-slate-700 hover:bg-slate-600 text-slate-200'}>Limpar filtros</button>
          <div className="flex-1" />
          {sub === 'tabela_anual' && (
            <button className={btnCls + ' bg-slate-700 hover:bg-slate-600 text-slate-200'}>Filtrar Estoque (Selecao)</button>
          )}
          <button className={btnCls + ' bg-blue-600 hover:bg-blue-500 text-white'}>Destacar</button>
          <button className={btnCls + ' bg-slate-700 hover:bg-slate-600 text-slate-200'}>Exportar Excel</button>
        </div>
        {/* Search row */}
        <div className="flex gap-2 mt-2 flex-wrap">
          <select className={inputCls + ' w-48'}><option>Filtrar id_agrupado</option></select>
          <input className={inputCls + ' flex-1'} placeholder="Filtrar descricao..." value={search} onChange={e => setSearch(e.target.value)} />
          {sub === 'tabela_mensal' && (
            <>
              <select className={inputCls + ' w-24'}><option>Ano: Todos</option></select>
              <select className={inputCls + ' w-24'}><option>Mes: Todos</option></select>
            </>
          )}
          {sub === 'tabela_anual' && (
            <select className={inputCls + ' w-24'}><option>Todos</option></select>
          )}
        </div>
        {data && (
          <div className="text-xs text-slate-400 mt-1">
            Exibindo {rows.length.toLocaleString('pt-BR')} de {data.total_rows.toLocaleString('pt-BR')} linhas.
          </div>
        )}
        <div className="text-xs text-slate-500 mt-0.5">Filtros ativos: nenhum</div>
      </div>

      <div className="flex-1 overflow-hidden">
        <DataTable
          columns={data?.columns ?? []}
          rows={rows}
          totalRows={data?.total_rows}
          loading={isLoading}
          page={page}
          totalPages={data?.total_pages}
          onPageChange={setPage}
          highlightRows={sub === 'mov_estoque'}
        />
      </div>
    </div>
  );
}

export function EstoqueTab() {
  const { selectedCnpj } = useAppStore();
  const [subTab, setSubTab] = useState<SubTab>('mov_estoque');

  const { data: movData } = useQuery({
    queryKey: ['mov_estoque_count', selectedCnpj],
    queryFn: () => estoqueApi.movEstoque(selectedCnpj!, 1, 1),
    enabled: !!selectedCnpj,
  });
  const { data: mensalData } = useQuery({
    queryKey: ['tabela_mensal_count', selectedCnpj],
    queryFn: () => estoqueApi.tabelaMensal(selectedCnpj!, 1, 1),
    enabled: !!selectedCnpj,
  });
  const { data: anualData } = useQuery({
    queryKey: ['tabela_anual_count', selectedCnpj],
    queryFn: () => estoqueApi.tabelaAnual(selectedCnpj!, 1, 1),
    enabled: !!selectedCnpj,
  });
  const { data: idAgrData } = useQuery({
    queryKey: ['id_agrupados_count', selectedCnpj],
    queryFn: () => estoqueApi.idAgrupados(selectedCnpj!, 1, 1),
    enabled: !!selectedCnpj,
  });

  if (!selectedCnpj) {
    return <div className="flex items-center justify-center h-full text-slate-500">Selecione um CNPJ.</div>;
  }

  const subtabs: { key: SubTab; label: string; count?: number }[] = [
    { key: 'mov_estoque', label: 'Tabela mov_estoque', count: movData?.total_rows },
    { key: 'tabela_mensal', label: 'Tabela mensal', count: mensalData?.total_rows },
    { key: 'tabela_anual', label: 'Tabela anual', count: anualData?.total_rows },
    { key: 'resumo', label: 'Resumo Global' },
    { key: 'produtos', label: 'Produtos selecionados', count: idAgrData?.total_rows },
    { key: 'id_agrupados', label: 'id_agrupados', count: idAgrData?.total_rows },
  ];

  return (
    <div className="flex flex-col h-full">
      {/* Subtab bar */}
      <div className="flex gap-1 px-3 pt-2 border-b border-slate-700" style={{ background: '#0a1628' }}>
        {subtabs.map((st) => (
          <button
            key={st.key}
            onClick={() => setSubTab(st.key)}
            className={`px-3 py-1.5 rounded-t text-xs font-medium transition-colors border-t border-l border-r ${
              subTab === st.key
                ? 'border-slate-600 text-white'
                : 'border-transparent text-slate-400 hover:text-slate-200'
            }`}
            style={subTab === st.key ? { background: '#0f1b33' } : {}}
          >
            {st.label}{st.count !== undefined ? ` (${st.count.toLocaleString('pt-BR')})` : ''}
          </button>
        ))}
      </div>

      <div className="flex-1 overflow-hidden">
        <EstoqueSubTab cnpj={selectedCnpj} sub={subTab} />
      </div>
    </div>
  );
}
