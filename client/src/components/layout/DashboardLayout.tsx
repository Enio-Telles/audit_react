/*
 * DashboardLayout - Swiss Design Fiscal
 * Sidebar escura fixa (240px) + area de trabalho off-white
 * Navegacao por icone + texto, header contextual
 */
import { useState, type ReactNode } from "react";
import { Link, useLocation } from "wouter";
import {
  LayoutDashboard,
  Database,
  Table2,
  Boxes,
  ArrowLeftRight,
  Package,
  Settings,
  ChevronLeft,
  ChevronRight,
  Shield,
  FileText,
  GitBranch,
  Search,
  IdCard,
} from "lucide-react";
import { cn } from "@/lib/utils";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { Input } from "@/components/ui/input";
import { formatarCnpj, useCnpj } from "@/contexts/CnpjContext";

interface NavItem {
  path: string;
  label: string;
  icon: typeof LayoutDashboard;
  description: string;
}

const navItems: NavItem[] = [
  {
    path: "/dashboard",
    label: "Dashboard",
    icon: LayoutDashboard,
    description: "Visao geral e atalhos",
  },
  {
    path: "/dados-cadastrais",
    label: "Dados Cadastrais",
    icon: IdCard,
    description: "Ficha cadastral do documento em analise",
  },
  {
    path: "/extracao",
    label: "Extracao",
    icon: Database,
    description: "Extracao Oracle e pipeline",
  },
  {
    path: "/consulta",
    label: "Consulta",
    icon: Table2,
    description: "Visualizar tabelas Parquet",
  },
  {
    path: "/agregacao-conversao",
    label: "Agregacao e Conversao",
    icon: Boxes,
    description: "Agrupar e converter produtos e unidades",
  },
  {
    path: "/estoque",
    label: "Estoque",
    icon: Package,
    description: "Movimentacao e saldos",
  },
  {
    path: "/mapeamento-oracle",
    label: "Mapa Oracle",
    icon: GitBranch,
    description: "Raiz SQL, Parquets e Polars",
  },
  {
    path: "/relatorios",
    label: "Relatorios",
    icon: FileText,
    description: "Relatorios fiscais conclusivos",
  },
];

const bottomNavItems: NavItem[] = [
  {
    path: "/",
    label: "Entrada",
    icon: Search,
    description: "Selecionar CNPJ, CPF ou lote",
  },
  {
    path: "/configuracoes",
    label: "Configuracoes",
    icon: Settings,
    description: "Preferencias do sistema",
  },
];

function NavLink({
  item,
  collapsed,
  isActive,
}: {
  item: NavItem;
  collapsed: boolean;
  isActive: boolean;
}) {
  const content = (
    <Link
      href={item.path}
      className={cn(
        "flex items-center gap-3 rounded-md px-3 py-2.5 text-sm font-medium transition-colors duration-150",
        isActive
          ? "bg-sidebar-accent text-sidebar-primary-foreground"
          : "text-sidebar-foreground/70 hover:bg-sidebar-accent/50 hover:text-sidebar-foreground"
      )}
    >
      <item.icon
        className={cn("shrink-0", collapsed ? "h-5 w-5" : "h-4.5 w-4.5")}
      />
      {!collapsed && <span className="truncate">{item.label}</span>}
    </Link>
  );

  if (collapsed) {
    return (
      <Tooltip delayDuration={0}>
        <TooltipTrigger asChild>{content}</TooltipTrigger>
        <TooltipContent side="right" className="font-medium">
          <p>{item.label}</p>
          <p className="text-xs font-normal text-muted-foreground">
            {item.description}
          </p>
        </TooltipContent>
      </Tooltip>
    );
  }

  return content;
}

export default function DashboardLayout({ children }: { children: ReactNode }) {
  const [location] = useLocation();
  const [collapsed, setCollapsed] = useState(false);
  const { cnpjAtivo, definirCnpjAtivo } = useCnpj();

  const currentPage =
    [...navItems, ...bottomNavItems].find(item => item.path === location) ??
    navItems[0];

  return (
    <div className="flex h-screen overflow-hidden">
      <aside
        className={cn(
          "flex shrink-0 flex-col border-r border-sidebar-border bg-sidebar transition-all duration-200",
          collapsed ? "w-16" : "w-60"
        )}
      >
        <div
          className={cn(
            "flex h-14 shrink-0 items-center gap-3 border-b border-sidebar-border px-4",
            collapsed && "justify-center px-2"
          )}
        >
          <Link
            href="/"
            className={cn(
              "flex items-center gap-3",
              collapsed && "justify-center"
            )}
          >
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-sidebar-primary">
              <Shield className="h-4.5 w-4.5 text-sidebar-primary-foreground" />
            </div>
            {!collapsed ? (
              <div className="flex min-w-0 flex-col">
                <span className="truncate text-sm font-bold tracking-tight text-sidebar-foreground">
                  Audit React
                </span>
                <span className="text-[10px] font-medium uppercase tracking-wider text-sidebar-foreground/50">
                  SEFIN
                </span>
              </div>
            ) : null}
          </Link>
        </div>

        <nav className="flex flex-1 flex-col gap-0.5 overflow-y-auto px-2 py-3">
          {navItems.map(item => (
            <NavLink
              key={item.path}
              item={item}
              collapsed={collapsed}
              isActive={location === item.path}
            />
          ))}
        </nav>

        <div className="space-y-0.5 px-2 pb-2">
          {bottomNavItems.map(item => (
            <NavLink
              key={item.path}
              item={item}
              collapsed={collapsed}
              isActive={location === item.path}
            />
          ))}
          <button
            onClick={() => setCollapsed(!collapsed)}
            className="flex w-full items-center gap-3 rounded-md px-3 py-2 text-sm text-sidebar-foreground/50 transition-colors hover:bg-sidebar-accent/50 hover:text-sidebar-foreground"
          >
            {collapsed ? (
              <ChevronRight className="h-4 w-4 shrink-0" />
            ) : (
              <>
                <ChevronLeft className="h-4 w-4 shrink-0" />
                <span>Recolher</span>
              </>
            )}
          </button>
        </div>
      </aside>

      <div className="flex flex-1 flex-col overflow-hidden">
        <header className="flex h-14 shrink-0 items-center justify-between border-b border-border bg-background px-6">
          <div>
            <h1 className="text-base font-bold tracking-tight text-foreground">
              {currentPage.label}
            </h1>
            <p className="text-xs text-muted-foreground">
              {currentPage.description}
            </p>
          </div>
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2 rounded-md bg-muted px-3 py-1.5 text-xs">
              <span className="text-muted-foreground">CNPJ ativo</span>
              <Input
                value={formatarCnpj(cnpjAtivo)}
                onChange={event => definirCnpjAtivo(event.target.value)}
                placeholder="00.000.000/0000-00"
                className="h-7 w-44 border-0 bg-background font-mono text-xs"
              />
            </div>
          </div>
        </header>

        <main className="flex-1 overflow-y-auto p-6">{children}</main>
      </div>
    </div>
  );
}
