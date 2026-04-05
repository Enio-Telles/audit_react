import { create } from 'zustand';
import type { FilterItem, ParquetFile } from '../api/types';

interface AppStore {
  // CNPJ selection
  selectedCnpj: string | null;
  setSelectedCnpj: (cnpj: string | null) => void;

  // File selection
  selectedFile: ParquetFile | null;
  setSelectedFile: (file: ParquetFile | null) => void;

  // Active tab
  activeTab: string;
  setActiveTab: (tab: string) => void;

  // Consulta tab state
  consultaFilters: FilterItem[];
  addConsultaFilter: (f: FilterItem) => void;
  removeConsultaFilter: (idx: number) => void;
  clearConsultaFilters: () => void;
  consultaVisibleCols: string[];
  setConsultaVisibleCols: (cols: string[]) => void;
  consultaPage: number;
  setConsultaPage: (p: number) => void;

  // Left panel visibility
  leftPanelVisible: boolean;
  toggleLeftPanel: () => void;
}

export const useAppStore = create<AppStore>((set) => ({
  selectedCnpj: null,
  setSelectedCnpj: (cnpj) => set({ selectedCnpj: cnpj, selectedFile: null, consultaPage: 1, consultaFilters: [], consultaVisibleCols: [] }),

  selectedFile: null,
  setSelectedFile: (file) => set({ selectedFile: file, consultaPage: 1, consultaFilters: [], consultaVisibleCols: [] }),

  activeTab: 'consulta',
  setActiveTab: (tab) => set({ activeTab: tab }),

  consultaFilters: [],
  addConsultaFilter: (f) => set((s) => ({ consultaFilters: [...s.consultaFilters, f] })),
  removeConsultaFilter: (idx) => set((s) => ({ consultaFilters: s.consultaFilters.filter((_, i) => i !== idx) })),
  clearConsultaFilters: () => set({ consultaFilters: [] }),

  consultaVisibleCols: [],
  setConsultaVisibleCols: (cols) => set({ consultaVisibleCols: cols }),

  consultaPage: 1,
  setConsultaPage: (p) => set({ consultaPage: p }),

  leftPanelVisible: true,
  toggleLeftPanel: () => set((s) => ({ leftPanelVisible: !s.leftPanelVisible })),
}));
