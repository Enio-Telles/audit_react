import { useState, useCallback, useMemo } from "react";
import {
  DndContext,
  closestCenter,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
} from "@dnd-kit/core";
import type { DragEndEvent } from "@dnd-kit/core";
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";

interface DossieColumnManagerProps {
  open: boolean;
  onClose: () => void;
  columns: string[];
  columnOrder: string[];
  hiddenColumns: Set<string>;
  onReorder: (newOrder: string[]) => void;
  onToggleVisibility: (column: string) => void;
  onShowAll: () => void;
  onHideAll: () => void;
}

// ─── Sortable Column Item ────────────────────────────────────────────────────
function SortableColumnItem({
  column,
  isVisible,
  onToggle,
  onMoveUp,
  onMoveDown,
  isFirst,
  isLast,
}: {
  column: string;
  isVisible: boolean;
  onToggle: () => void;
  onMoveUp: () => void;
  onMoveDown: () => void;
  isFirst: boolean;
  isLast: boolean;
}) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id: column });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
  };

  return (
    <div
      ref={setNodeRef}
      style={style}
      className={`flex items-center gap-2 rounded-lg border px-2.5 py-1.5 text-xs transition-colors ${
        isDragging
          ? "border-blue-500/50 bg-blue-950/30"
          : "border-slate-700/60 bg-slate-900/50 hover:bg-slate-800/50"
      }`}
    >
      {/* Drag handle */}
      <button
        type="button"
        {...attributes}
        {...listeners}
        className="cursor-grab touch-none text-slate-500 hover:text-slate-300 active:cursor-grabbing"
        title="Arrastar para reordenar"
      >
        ⠿
      </button>

      {/* Visibility checkbox */}
      <label className="flex flex-1 cursor-pointer items-center gap-2">
        <input
          type="checkbox"
          checked={isVisible}
          onChange={onToggle}
          className="h-3.5 w-3.5 rounded border-slate-600 bg-slate-900 text-blue-500 focus:ring-0 focus:ring-offset-0"
        />
        <span
          className={`truncate ${isVisible ? "text-slate-200" : "text-slate-500 line-through"}`}
        >
          {column}
        </span>
      </label>

      {/* Arrow buttons */}
      <div className="flex shrink-0 gap-0.5">
        <button
          type="button"
          onClick={onMoveUp}
          disabled={isFirst}
          className="rounded p-0.5 text-[10px] text-slate-500 transition-colors hover:text-slate-200 disabled:cursor-not-allowed disabled:text-slate-700"
          title="Mover para cima"
        >
          ▲
        </button>
        <button
          type="button"
          onClick={onMoveDown}
          disabled={isLast}
          className="rounded p-0.5 text-[10px] text-slate-500 transition-colors hover:text-slate-200 disabled:cursor-not-allowed disabled:text-slate-700"
          title="Mover para baixo"
        >
          ▼
        </button>
      </div>
    </div>
  );
}

export function DossieColumnManager({
  open,
  onClose,
  columns,
  columnOrder,
  hiddenColumns,
  onReorder,
  onToggleVisibility,
  onShowAll,
  onHideAll,
}: DossieColumnManagerProps) {
  const [searchTerm, setSearchTerm] = useState("");

  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 5 } }),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    }),
  );

  const handleDragEnd = useCallback(
    (event: DragEndEvent) => {
      const { active, over } = event;
      if (over && active.id !== over.id) {
        const oldIndex = columnOrder.indexOf(String(active.id));
        const newIndex = columnOrder.indexOf(String(over.id));
        onReorder(arrayMove(columnOrder, oldIndex, newIndex));
      }
    },
    [columnOrder, onReorder],
  );

  const moveColumn = useCallback(
    (column: string, direction: -1 | 1) => {
      const idx = columnOrder.indexOf(column);
      const newIdx = idx + direction;
      if (newIdx < 0 || newIdx >= columnOrder.length) return;
      onReorder(arrayMove(columnOrder, idx, newIdx));
    },
    [columnOrder, onReorder],
  );

  const filteredColumns = useMemo(() => {
    // ⚡ Bolt Optimization: Hoist lowercasing outside filter loop to prevent O(N) string allocations per re-render
    const searchLower = searchTerm ? searchTerm.toLowerCase() : "";
    if (!searchLower) return columnOrder;

    return columnOrder.filter((col) =>
      col.toLowerCase().includes(searchLower),
    );
  }, [columnOrder, searchTerm]);

  if (!open) return null;

  const visibleCount = columns.filter((c) => !hiddenColumns.has(c)).length;

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 z-40 bg-black/40 backdrop-blur-sm"
        onClick={onClose}
      />

      {/* Drawer */}
      <div className="fixed right-0 top-0 z-50 flex h-full w-80 flex-col border-l border-slate-700 bg-slate-950 shadow-2xl">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-slate-800 px-4 py-3">
          <div>
            <h3 className="text-sm font-semibold text-white">
              Gestão de colunas
            </h3>
            <p className="mt-0.5 text-[11px] text-slate-400">
              {visibleCount}/{columns.length} visíveis
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded-md p-1.5 text-xs text-slate-400 transition-colors hover:bg-slate-800 hover:text-slate-200"
          >
            ✕
          </button>
        </div>

        {/* Search + bulk actions */}
        <div className="border-b border-slate-800 px-4 py-2.5">
          <input
            type="text"
            placeholder="Buscar coluna..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="mb-2 w-full rounded-lg border border-slate-700 bg-slate-900 px-3 py-1.5 text-xs text-slate-200 placeholder-slate-500 focus:border-blue-500 focus:outline-none"
          />
          <div className="flex gap-2">
            <button
              type="button"
              onClick={onShowAll}
              className="rounded-md border border-slate-700 bg-slate-900 px-2.5 py-1 text-[11px] text-slate-300 transition-colors hover:bg-slate-800"
            >
              Mostrar todas
            </button>
            <button
              type="button"
              onClick={onHideAll}
              className="rounded-md border border-slate-700 bg-slate-900 px-2.5 py-1 text-[11px] text-slate-300 transition-colors hover:bg-slate-800"
            >
              Ocultar todas
            </button>
          </div>
        </div>

        {/* Column list with DnD */}
        <div className="flex-1 overflow-y-auto px-4 py-3">
          <DndContext
            sensors={sensors}
            collisionDetection={closestCenter}
            onDragEnd={handleDragEnd}
          >
            <SortableContext
              items={filteredColumns}
              strategy={verticalListSortingStrategy}
            >
              <div className="space-y-1.5">
                {filteredColumns.map((column, index) => (
                  <SortableColumnItem
                    key={column}
                    column={column}
                    isVisible={!hiddenColumns.has(column)}
                    onToggle={() => onToggleVisibility(column)}
                    onMoveUp={() => moveColumn(column, -1)}
                    onMoveDown={() => moveColumn(column, 1)}
                    isFirst={index === 0}
                    isLast={index === filteredColumns.length - 1}
                  />
                ))}
              </div>
            </SortableContext>
          </DndContext>
        </div>

        {/* Footer */}
        <div className="border-t border-slate-800 px-4 py-3">
          <p className="text-[10px] text-slate-500">
            Arraste ⠿ ou use ▲▼ para reordenar. Checkbox para visibilidade.
          </p>
        </div>
      </div>
    </>
  );
}
