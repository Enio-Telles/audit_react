import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { DossieTab } from './DossieTab';

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
});
