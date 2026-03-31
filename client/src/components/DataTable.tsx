import React, { useMemo, useState, useCallback, useRef } from "react";
import { AgGridReact } from "ag-grid-react";
import {
  ColDef,
  ModuleRegistry,
  AllCommunityModule,
  themeBalham,
  RowClassParams,
} from "ag-grid-community";
import { Button } from "@/components/ui/button";
import {
  Popover,
  PopoverTrigger,
  PopoverContent,
} from "@/components/ui/popover";
import { PaintBucket } from "lucide-react";

ModuleRegistry.registerModules([AllCommunityModule]);

interface DataTableProps {
  dados: any[];
  colunas?: string[]; // Either colunas OR customColumnDefs should be provided
  customColumnDefs?: ColDef[];
  onSelectionChanged?: (selectedNodes: any[]) => void;
  rowSelection?: "single" | "multiRow" | "multiple";
  frameworkComponents?: any;
}

const COLORS = [
  "#f87171", // red
  "#fb923c", // orange
  "#facc15", // yellow
  "#4ade80", // green
  "#60a5fa", // blue
  "#c084fc", // purple
  "#f472b6", // pink
  "transparent",
];

export function DataTable({
  dados,
  colunas,
  customColumnDefs,
  onSelectionChanged,
  rowSelection = "multiRow",
  frameworkComponents,
}: DataTableProps) {
  const gridRef = useRef<any>(null);
  const [rowColors, setRowColors] = useState<Record<string, string>>({});
  const [colColors, setColColors] = useState<Record<string, string>>({});

  const applyColorToSelected = (color: string) => {
    if (!gridRef.current) return;
    const api = gridRef.current.api;
    const selectedNodes = api.getSelectedNodes();
    const focusedCell = api.getFocusedCell();

    if (selectedNodes.length > 0) {
      const newRowColors = { ...rowColors };
      selectedNodes.forEach((node: any) => {
        if (node.id) newRowColors[node.id] = color;
      });
      setRowColors(newRowColors);
      api.redrawRows({ rowNodes: selectedNodes });
    } else if (focusedCell) {
      const colId = focusedCell.column.getColId();
      setColColors((prev: any) => ({ ...prev, [colId]: color }));
      api.refreshCells({ columns: [colId], force: true });
    }
  };

  const getRowStyle = useCallback(
    (params: RowClassParams) => {
      if (params.node.id && rowColors[params.node.id]) {
        if (rowColors[params.node.id] !== "transparent") {
          return { backgroundColor: rowColors[params.node.id] };
        }
      }
      return undefined;
    },
    [rowColors]
  );

  const columnDefs = useMemo<ColDef[]>(() => {
    if (customColumnDefs) {
      return customColumnDefs.map(def => ({
        ...def,
        cellStyle: (params: any) => {
          let baseStyle =
            typeof def.cellStyle === "function"
              ? def.cellStyle(params)
              : def.cellStyle;
          baseStyle = baseStyle || {};

          const colId = params.column.getColId();
          if (colColors[colId] && colColors[colId] !== "transparent") {
            return { ...baseStyle, backgroundColor: colColors[colId] };
          }
          return baseStyle;
        },
      }));
    }

    if (!colunas) return [];

    return colunas.map((col, index) => ({
      field: col,
      headerName: col,
      filter: true,
      sortable: true,
      resizable: true,
      cellStyle: (params: any) => {
        const colId = params.column.getColId();
        if (colColors[colId] && colColors[colId] !== "transparent") {
          return { backgroundColor: colColors[colId] };
        }
        return null;
      },
    }));
  }, [colunas, colColors, customColumnDefs]);

  const defaultColDef = useMemo<ColDef>(() => {
    return {
      flex: 1,
      minWidth: 100,
    };
  }, []);

  const theme = themeBalham.withParams({
    headerBackgroundColor: "hsl(var(--muted) / 0.5)",
    headerTextColor: "hsl(var(--foreground))",
    foregroundColor: "hsl(var(--foreground))",
    backgroundColor: "hsl(var(--background))",
    borderColor: "hsl(var(--border))",
  });

  const CustomGrid = AgGridReact as any;

  return (
    <div className="flex flex-col gap-2 w-full">
      <div className="flex justify-end">
        <Popover>
          <PopoverTrigger asChild>
            <Button
              variant="outline"
              size="sm"
              className="gap-2 cursor-pointer"
            >
              <PaintBucket className="h-4 w-4" />
              Destacar Cores
            </Button>
          </PopoverTrigger>
          <PopoverContent className="w-auto p-2" align="end">
            <div className="flex gap-1">
              {COLORS.map(c => (
                <button
                  key={c}
                  className="w-6 h-6 rounded border cursor-pointer flex items-center justify-center text-xs"
                  style={{ backgroundColor: c === "transparent" ? "#fff" : c }}
                  onClick={() => applyColorToSelected(c)}
                  title={c === "transparent" ? "Remover cor" : "Aplicar cor"}
                >
                  {c === "transparent" && "❌"}
                </button>
              ))}
            </div>
            <p className="text-xs text-muted-foreground mt-2 max-w-[200px]">
              Selecione as linhas ou clique em uma célula para colorir a coluna.
            </p>
          </PopoverContent>
        </Popover>
      </div>
      <div
        style={{ height: 400, width: "100%" }}
        className="ag-theme-balham rounded border"
      >
        <CustomGrid
          ref={gridRef}
          rowData={dados}
          columnDefs={columnDefs}
          defaultColDef={defaultColDef}
          theme={theme}
          getRowId={(params: any) => {
            if (params.data.id_item) return String(params.data.id_item);
            if (params.data.id) return String(params.data.id);
            if (params.data.cnpj) return String(params.data.cnpj);
            return String(Math.random());
          }}
          getRowStyle={getRowStyle}
          rowSelection={{
            mode: rowSelection,
            headerCheckbox:
              rowSelection === "multiRow" || rowSelection === "multiple",
          }}
          onSelectionChanged={(event: any) => {
            if (onSelectionChanged) {
              onSelectionChanged(
                event.api.getSelectedNodes().map((n: any) => n.data)
              );
            }
          }}
          enableCellTextSelection={true}
          components={frameworkComponents}
        />
      </div>
    </div>
  );
}
