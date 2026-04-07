import { useQuery } from '@tanstack/react-query';
import { dossieApi } from '../../../api/client';
import type { DossieSectionSummary, DossieTabProps } from '../types';

function statusLabel(status: DossieSectionSummary['status']): string {
  switch (status) {
    case 'cached':
      return 'Cache disponível';
    case 'loading':
      return 'Carregando';
    case 'fresh':
      return 'Atualizado';
    case 'error':
      return 'Erro';
    default:
      return 'Aguardando';
  }
}

export function DossieTab({ cnpj, razaoSocial }: DossieTabProps) {
  const { data: sections, isLoading, isError } = useQuery({
    queryKey: ['dossie_sections', cnpj],
    queryFn: () => dossieApi.getSecoes(cnpj!),
    enabled: !!cnpj,
  });

  if (!cnpj) {
    return (
      <div className="h-full w-full flex items-center justify-center p-6 text-slate-300">
        <div className="max-w-xl rounded-2xl border border-slate-700 bg-slate-900/60 p-6">
          <h2 className="text-lg font-semibold text-white mb-2">Dossiê indisponível</h2>
          <p className="text-sm text-slate-400">
            Selecione um CNPJ para abrir o dossiê, reaproveitar extrações já existentes e evitar duplicação de dados.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full overflow-auto p-4 text-slate-200">
      <div className="mb-4 rounded-2xl border border-slate-700 bg-slate-900/60 p-4">
        <div className="text-xs uppercase tracking-wide text-slate-400 mb-1">Dossiê principal</div>
        <h2 className="text-lg font-semibold text-white">{cnpj}</h2>
        {razaoSocial && <div className="text-sm text-slate-400 mt-1">{razaoSocial}</div>}
        <p className="text-sm text-slate-400 mt-3">
          Esta área concentra a navegação do dossiê por CNPJ, priorizando reuso de consultas SQL, persistência por seção e leitura amigável dos dados.
        </p>
      </div>

      {isLoading && <div className="text-sm text-slate-400">Carregando seções do dossiê...</div>}
      {isError && <div className="text-sm text-red-400">Erro ao carregar as seções do dossiê.</div>}

      {!isLoading && !isError && sections && (
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
          {sections.map((section: any) => (
            <div key={section.id} className="rounded-2xl border border-slate-700 bg-slate-900/50 p-4 hover:bg-slate-800/80 transition-colors cursor-pointer border-t-2 hover:border-t-blue-500">
              <div className="flex items-center justify-between gap-2 mb-2">
                <h3 className="text-sm font-semibold text-white">{section.title}</h3>
                <span className={`rounded-full border px-2 py-0.5 text-[10px] ${
                  section.status === 'cached' || section.status === 'fresh'
                    ? 'border-green-800 text-green-300 bg-green-900/30'
                    : 'border-slate-600 text-slate-300'
                }`}>
                  {statusLabel(section.status)}
                </span>
              </div>
              <p className="text-sm text-slate-400 mb-3">{section.description}</p>
              <div className="flex items-center justify-between text-xs text-slate-500">
                <span>Fonte: {section.sourceType}</span>
                <span>{section.rowCount ? `${section.rowCount} linhas` : 'sem carga'}</span>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export default DossieTab;
