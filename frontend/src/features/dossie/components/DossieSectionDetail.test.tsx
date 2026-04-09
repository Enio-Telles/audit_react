import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { DossieSectionData } from '../types';
import { useAppStore } from '../../../store/appStore';
import { DossieSectionDetail } from './DossieSectionDetail';

const dataTableSpy = vi.fn();

vi.mock('../../../api/client', () => ({
  dossieApi: {
    getHistoricoComparacoesContato: vi.fn(() =>
      Promise.resolve({
        cnpj: '12345678000190',
        secaoId: 'contato',
        items: [],
        historyFile:
          'CNPJ/12345678000190/arquivos_parquet/dossie/historico_comparacao_contato_12345678000190.jsonl',
      }),
    ),
    getResumoComparacoesContato: vi.fn(() =>
      Promise.resolve({
        cnpj: '12345678000190',
        secaoId: 'contato',
        totalComparacoes: 4,
        convergenciasFuncionais: 2,
        divergenciasFuncionais: 1,
        convergenciasBasicas: 1,
        divergenciasBasicas: 0,
        ultimaEstrategia: 'sql_consolidado',
        ultimaSqlPrincipal: 'dossie_contato.sql',
        ultimaEstrategiaReferencia: 'composicao_polars',
        ultimaSqlPrincipalReferencia: 'dados_cadastrais.sql',
        ultimoStatusComparacao: 'divergencia_funcional',
        ultimoCacheKey: 'cache_a3',
        ultimoTotalChavesFaltantes: 2,
        ultimoTotalChavesExtras: 1,
        historyFile:
          'CNPJ/12345678000190/arquivos_parquet/dossie/historico_comparacao_contato_12345678000190.jsonl',
      }),
    ),
    gerarRelatorioComparacoesContato: vi.fn(),
  },
}));

vi.mock('./DossieContatoDetalhe', () => ({
  DossieContatoDetalhe: () => <div>Contato mockado</div>,
}));

vi.mock('../../../components/table/DataTable', () => ({
  DataTable: (props: {
    sortBy?: string;
    sortDesc?: boolean;
    onSortChange?: (col: string, desc: boolean) => void;
    columnFilters?: Record<string, string>;
    onColumnFilterChange?: (col: string, value: string) => void;
  }) => {
    dataTableSpy(props);
    return (
      <div>
        <div data-testid="sort-by">{props.sortBy ?? 'sem-ordenacao'}</div>
        <div data-testid="sort-desc">{String(props.sortDesc ?? false)}</div>
        <div data-testid="filtro-id">{props.columnFilters?.id_linha_origem ?? ''}</div>
        <button
          type="button"
          onClick={() => props.onSortChange?.('id_linha_origem', true)}
        >
          Simular ordenacao
        </button>
        <button
          type="button"
          onClick={() => props.onColumnFilterChange?.('id_linha_origem', 'ABC123')}
        >
          Simular filtro
        </button>
      </div>
    );
  },
}));

function criarWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  });

  return ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
}

function criarDadosNaoContato(): DossieSectionData {
  return {
    id: 'documentos',
    title: 'Documentos',
    columns: ['id_linha_origem', 'origem_dado', 'valor_total'],
    rows: [
      {
        id_linha_origem: 'LINHA-1',
        origem_dado: 'dados_cadastrais.sql',
        valor_total: 10,
      },
    ],
    rowCount: 1,
    cacheFile:
      'CNPJ/12345678000190/arquivos_parquet/dossie/dossie_12345678000190_documentos.parquet',
    metadata: null,
    updatedAt: '2026-04-08T10:00:00',
  };
}

function criarDadosContato(): DossieSectionData {
  return {
    id: 'contato',
    title: 'Contato',
    columns: ['tipo_vinculo', 'nome_referencia', 'telefone'],
    rows: [
      {
        tipo_vinculo: 'EMPRESA_PRINCIPAL',
        nome_referencia: 'Empresa Teste',
        telefone: '62999990000',
      },
    ],
    rowCount: 1,
    cacheFile:
      'CNPJ/12345678000190/arquivos_parquet/dossie/dossie_12345678000190_contato.parquet',
    metadata: {
      estrategia_execucao: 'sql_consolidado',
      sql_principal: 'dossie_contato.sql',
      comparison_history_file:
        'CNPJ/12345678000190/arquivos_parquet/dossie/historico_comparacao_contato_12345678000190.jsonl',
      comparacao_estrategia_alternativa: {
        convergencia_funcional: false,
        estrategia_referencia: 'composicao_polars',
        sql_principal_referencia: 'dados_cadastrais.sql',
        quantidade_chaves_faltantes: 2,
        quantidade_chaves_extras: 1,
      },
    },
    updatedAt: '2026-04-09T10:00:00',
  };
}

describe('DossieSectionDetail', () => {
  beforeEach(async () => {
    vi.clearAllMocks();
    window.localStorage.clear();
    useAppStore.setState({
      dossieViewMode: 'executivo',
      dossieTableProfile: 'compacto',
      dossieUsarSqlConsolidadoContato: false,
      dossieSectionTableStateById: {},
    });
    await useAppStore.persist.rehydrate();
  });

  it('rehidrata e persiste ordenacao/filtros por secao no detalhe tabular', async () => {
    window.localStorage.setItem(
      'fiscal-parquet-app-store',
      JSON.stringify({
        state: {
          dossieViewMode: 'executivo',
          dossieTableProfile: 'compacto',
          dossieUsarSqlConsolidadoContato: false,
          dossieSectionTableStateById: {
            '12345678000190:documentos': {
              sortBy: 'id_linha_origem',
              sortDesc: false,
              columnFilters: {
                id_linha_origem: 'PRESET',
              },
            },
          },
        },
        version: 0,
      }),
    );
    await useAppStore.persist.rehydrate();

    const wrapper = criarWrapper();
    render(<DossieSectionDetail dados={criarDadosNaoContato()} />, { wrapper });

    expect(screen.getByTestId('sort-by')).toHaveTextContent('id_linha_origem');
    expect(screen.getByTestId('sort-desc')).toHaveTextContent('false');
    expect(screen.getByTestId('filtro-id')).toHaveTextContent('PRESET');

    fireEvent.click(screen.getByRole('button', { name: 'Simular ordenacao' }));
    fireEvent.click(screen.getByRole('button', { name: 'Simular filtro' }));

    await waitFor(() => {
      const estado =
        useAppStore.getState().dossieSectionTableStateById[
          '12345678000190:documentos'
        ];
      expect(estado.sortBy).toBe('id_linha_origem');
      expect(estado.sortDesc).toBe(true);
      expect(estado.columnFilters.id_linha_origem).toBe('ABC123');
    });
  });

  it('organiza resumo operacional e metadata tecnica no modo auditoria', async () => {
    useAppStore.setState({
      dossieViewMode: 'auditoria',
      dossieTableProfile: 'compacto',
    });

    const wrapper = criarWrapper();
    render(
      <DossieSectionDetail
        dados={{
          ...criarDadosNaoContato(),
          metadata: {
            estrategia_execucao: 'composicao_polars',
            sql_principal: 'dados_cadastrais.sql',
            sql_ids_executados: ['dados_cadastrais.sql'],
            sql_ids_reutilizados: ['cadastro_base.sql', 'contato_base.sql'],
            total_sql_ids: 3,
            percentual_reuso_sql: 66.67,
            tempo_materializacao_ms: 120,
            tempo_total_sync_ms: 180,
            impacto_cache_first: 'reduziu ida ao Oracle',
            tabela_origem: ['cadastro_base', 'contato_base'],
            comparacao_estrategia_alternativa: {
              convergencia_funcional: true,
              estrategia_referencia: 'sql_consolidado',
              sql_principal_referencia: 'dossie_documentos.sql',
              quantidade_chaves_faltantes: 0,
              quantidade_chaves_extras: 1,
            },
          },
        }}
        viewMode="auditoria"
      />,
      { wrapper },
    );

    expect(screen.getByText('Metadata tecnica')).toBeInTheDocument();
    expect(screen.getByText('Reuso Oracle')).toBeInTheDocument();
    expect(screen.getByText('66.67%')).toBeInTheDocument();
    expect(
      screen.getByText(/Convergencia funcional com estrategia alternada/i),
    ).toBeInTheDocument();
    expect(screen.getByText('SQL principal de referencia')).toBeInTheDocument();
    expect(screen.getByText('dossie_documentos.sql')).toBeInTheDocument();
  });

  it('exibe resumo consolidado da ultima comparacao de contato com referencias e chaves', async () => {
    useAppStore.setState({
      dossieViewMode: 'auditoria',
      dossieTableProfile: 'compacto',
    });

    const wrapper = criarWrapper();
    render(<DossieSectionDetail dados={criarDadosContato()} viewMode="auditoria" />, {
      wrapper,
    });

    expect(await screen.findByText('Resumo consolidado das comparacoes')).toBeInTheDocument();
    expect(screen.getByText(/Estrategia de referencia:/i)).toBeInTheDocument();
    expect(screen.getAllByText(/composicao_polars/i).length).toBeGreaterThan(0);
    expect(screen.getByText(/SQL principal de referencia:/i)).toBeInTheDocument();
    expect(screen.getAllByText(/dados_cadastrais.sql/i).length).toBeGreaterThan(0);
    expect(screen.getByText(/Ultimas chaves faltantes \/ extras:/i)).toBeInTheDocument();
    expect(screen.getAllByText(/2 \/ 1/).length).toBeGreaterThan(0);
  });
});
