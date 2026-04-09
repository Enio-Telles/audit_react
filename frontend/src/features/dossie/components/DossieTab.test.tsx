import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { DossieTab } from './DossieTab';
import { useAppStore } from '../../../store/appStore';

const getSecoes = vi.fn();
const getDadosSecao = vi.fn();
const syncSecao = vi.fn();

vi.mock('../../../api/client', () => ({
  dossieApi: {
    getSecoes: (...args: unknown[]) => getSecoes(...args),
    getDadosSecao: (...args: unknown[]) => getDadosSecao(...args),
    syncSecao: (...args: unknown[]) => syncSecao(...args),
  },
}));

vi.mock('./DossieSectionDetail', () => ({
  DossieSectionDetail: ({ dados }: { dados: { title: string } }) => <div>Detalhe mockado: {dados.title}</div>,
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

describe('DossieTab', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    window.localStorage.clear();
    useAppStore.setState({
      dossieViewMode: 'executivo',
      dossieTableProfile: 'compacto',
      dossieUsarSqlConsolidadoContato: false,
    });
  });

  it('atualiza o resumo sem reload manual e exibe mensagem de sucesso apos sync', async () => {
    getSecoes
      .mockResolvedValueOnce([
        {
          id: 'contato',
          title: 'Contato',
          description: 'Secao de contatos',
          sourceType: 'composed',
          syncEnabled: true,
          status: 'cached',
          rowCount: 1,
          updatedAt: '2026-04-08T10:00:00',
        },
      ])
      .mockResolvedValueOnce([
        {
          id: 'contato',
          title: 'Contato',
          description: 'Secao de contatos',
          sourceType: 'composed',
          syncEnabled: true,
          status: 'cached',
          rowCount: 3,
          executionStrategy: 'composicao_polars',
          primarySql: 'dados_cadastrais.sql',
          updatedAt: '2026-04-08T10:05:00',
        },
      ]);

    syncSecao.mockResolvedValue({
      status: 'success',
      cnpj: '12345678000190',
      secao_id: 'contato',
      linhas_extraidas: 3,
      cache_file: 'dossie_12345678000190_contato.parquet',
      estrategia_execucao: 'composicao_polars',
      sql_principal: 'dados_cadastrais.sql',
      sql_ids_executados: ['dossie_historico_socios.sql'],
      sql_ids_reutilizados: ['dados_cadastrais.sql'],
      comparacao_estrategia_alternativa: {
        convergencia_basica: true,
      },
    });

    const wrapper = criarWrapper();
    render(<DossieTab cnpj="12345678000190" razaoSocial="Empresa Teste" />, { wrapper });

    await screen.findByText('Contato');
    expect(screen.getByText('Nenhuma secao selecionada')).toBeInTheDocument();
    expect(screen.getByText('Status geral')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Executivo' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Auditoria' })).toBeInTheDocument();
    expect(screen.getByText(/Ultima atualizacao:/i)).toBeInTheDocument();
    expect(screen.getByText('1 linha')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Sincronizar' }));

    await waitFor(() => {
      expect(syncSecao).toHaveBeenCalledWith('12345678000190', 'contato', undefined);
    });

    await waitFor(() => {
      expect(screen.getByText(/3 linhas atualizadas/i)).toBeInTheDocument();
    });

    await waitFor(() => {
      expect(screen.getByText('3 linhas')).toBeInTheDocument();
    });

    expect(screen.getByText(/principal dados_cadastrais\.sql/i)).toBeInTheDocument();
    expect(getSecoes).toHaveBeenCalledTimes(2);
  });

  it('exibe mensagem de erro por secao quando o sync falha', async () => {
    getSecoes.mockResolvedValue([
      {
        id: 'contato',
        title: 'Contato',
        description: 'Secao de contatos',
        sourceType: 'composed',
        syncEnabled: true,
        status: 'cached',
        rowCount: 1,
        updatedAt: '2026-04-08T10:00:00',
      },
    ]);

    syncSecao.mockRejectedValue({
      response: {
        data: {
          detail: 'Falha simulada no sync',
        },
      },
    });

    const wrapper = criarWrapper();
    render(<DossieTab cnpj="12345678000190" razaoSocial="Empresa Teste" />, { wrapper });

    await screen.findByText('Contato');
    fireEvent.click(screen.getByRole('button', { name: 'Sincronizar' }));

    await waitFor(() => {
      expect(screen.getByText('Falha simulada no sync')).toBeInTheDocument();
    });
  });

  it('rehidrata preferencia de SQL consolidado e envia parametro no sync da secao contato', async () => {
    window.localStorage.setItem(
      'fiscal-parquet-app-store',
      JSON.stringify({
        state: {
          dossieViewMode: 'executivo',
          dossieTableProfile: 'compacto',
          dossieUsarSqlConsolidadoContato: true,
        },
        version: 0,
      }),
    );
    await useAppStore.persist.rehydrate();

    getSecoes.mockResolvedValue([
      {
        id: 'contato',
        title: 'Contato',
        description: 'Secao de contatos',
        sourceType: 'composed',
        syncEnabled: true,
        status: 'cached',
        rowCount: 1,
        updatedAt: '2026-04-08T10:00:00',
      },
    ]);

    syncSecao.mockResolvedValue({
      status: 'success',
      cnpj: '12345678000190',
      secao_id: 'contato',
      linhas_extraidas: 2,
      cache_file: 'dossie_12345678000190_contato.parquet',
      estrategia_execucao: 'sql_consolidado',
      sql_principal: 'dossie_contato.sql',
      sql_ids_executados: ['dossie_contato.sql'],
      sql_ids_reutilizados: [],
    });

    const wrapper = criarWrapper();
    render(<DossieTab cnpj="12345678000190" razaoSocial="Empresa Teste" />, { wrapper });

    await screen.findByText('Contato');
    const checkbox = screen.getByRole('checkbox');
    expect(checkbox).toBeChecked();

    fireEvent.click(screen.getByRole('button', { name: 'Sincronizar' }));

    await waitFor(() => {
      expect(syncSecao).toHaveBeenCalledWith('12345678000190', 'contato', {
        usar_sql_consolidado: true,
      });
    });
  });

  it('abre a secao prioritaria e permite sync em lote das pendentes', async () => {
    getSecoes.mockResolvedValue([
      {
        id: 'cadastro',
        title: 'Cadastro',
        description: 'Secao cadastral',
        sourceType: 'sql_catalog',
        syncEnabled: true,
        status: 'idle',
        rowCount: 0,
        updatedAt: null,
      },
      {
        id: 'contato',
        title: 'Contato',
        description: 'Secao de contatos',
        sourceType: 'composed',
        syncEnabled: true,
        status: 'error',
        rowCount: 1,
        updatedAt: '2026-04-08T10:00:00',
      },
    ]);

    getDadosSecao.mockResolvedValue({
      id: 'contato',
      title: 'Contato',
      columns: [],
      rows: [],
      rowCount: 0,
      cacheFile: 'CNPJ/12345678000190/arquivos_parquet/dossie/contato.parquet',
      metadata: null,
      updatedAt: '2026-04-08T10:00:00',
    });

    syncSecao.mockResolvedValue({
      status: 'success',
      cnpj: '12345678000190',
      secao_id: 'contato',
      linhas_extraidas: 1,
      cache_file: 'dossie_12345678000190_contato.parquet',
      estrategia_execucao: 'sql_direto',
      sql_ids_executados: ['dossie_contato.sql'],
      sql_ids_reutilizados: [],
    });

    const wrapper = criarWrapper();
    render(<DossieTab cnpj="12345678000190" razaoSocial="Empresa Teste" />, { wrapper });

    await screen.findByText('Contato');

    fireEvent.click(screen.getByRole('button', { name: 'Abrir secao prioritaria' }));

    await waitFor(() => {
      expect(screen.getByText('Detalhe mockado: Contato')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole('button', { name: 'Fechar painel' }));

    await waitFor(() => {
      expect(screen.getByText('Nenhuma secao selecionada')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole('button', { name: 'Abrir secao prioritaria' }));

    fireEvent.click(screen.getByRole('button', { name: 'Sincronizar pendentes' }));

    await waitFor(() => {
      expect(syncSecao).toHaveBeenNthCalledWith(1, '12345678000190', 'cadastro', undefined);
      expect(syncSecao).toHaveBeenNthCalledWith(2, '12345678000190', 'contato', undefined);
    });
  });
});
