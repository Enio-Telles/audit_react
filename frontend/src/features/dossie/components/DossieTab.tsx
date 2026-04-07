import type { DossieSectionSummary, DossieTabProps } from '../types';

const defaultSections: DossieSectionSummary[] = [
  {
    id: 'cadastro',
    title: 'Cadastro',
    description: 'Dados cadastrais, situação e histórico básico do contribuinte.',
    sourceType: 'mixed',
    status: 'idle',
  },
  {
    id: 'documentos_fiscais',
    title: 'Documentos fiscais',
    description: 'NF-e, NFC-e e outras visões reaproveitadas do catálogo SQL atual.',
    sourceType: 'sql_catalog',
    status: 'idle',
  },
  {
    id: 'arrecadacao',
    title: 'Arrecadação e conta corrente',
    description: 'Visões de situação fiscal e financeira com foco em reuso e rastreabilidade.',
    sourceType: 'mixed',
    status: 'idle',
  },
];

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

export function DossieTab({ cnpj, razaoSocial, sections }: DossieTabProps) {
  const items = sections && sections.length > 0 ? sections : defaultSections;

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

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        {items.map((section) => (
          <div key={section.id} className="rounded-2xl border border-slate-700 bg-slate-900/50 p-4">
            <div className="flex items-center justify-between gap-2 mb-2">
              <h3 className="text-sm font-semibold text-white">{section.title}</h3>
              <span className="rounded-full border border-slate-600 px-2 py-0.5 text-[10px] text-slate-300">
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
    </div>
  );
}

export default DossieTab;
