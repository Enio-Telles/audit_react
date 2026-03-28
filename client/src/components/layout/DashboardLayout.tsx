/*
 * DashboardLayout — Swiss Design Fiscal
 * Sidebar escura fixa (240px) + área de trabalho off-white
 * Navegação por ícone+texto, header contextual
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
} from "lucide-react";
import { cn } from "@/lib/utils";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";

interface NavItem {
  path: string;
  label: string;
  icon: typeof LayoutDashboard;
  description: string;
}

const navItems: NavItem[] = [
  {
    path: "/",
    label: "Dashboard",
    icon: LayoutDashboard,
    description: "Visão geral e atalhos",
  },
  {
    path: "/extracao",
    label: "Extração",
    icon: Database,
    description: "Extração Oracle e pipeline",
  },
  {
    path: "/consulta",
    label: "Consulta",
    icon: Table2,
    description: "Visualizar tabelas Parquet",
  },
  {
    path: "/agregacao",
    label: "Agregação",
    icon: Boxes,
    description: "Agrupar produtos",
  },
  {
    path: "/conversao",
    label: "Conversão",
    icon: ArrowLeftRight,
    description: "Fatores e unidades",
  },
  {
    path: "/estoque",
    label: "Estoque",
    icon: Package,
    description: "Movimentação e saldos",
  },
];

const bottomNavItems: NavItem[] = [
  {
    path: "/configuracoes",
    label: "Configurações",
    icon: Settings,
    description: "Preferências do sistema",
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
        "flex items-center gap-3 px-3 py-2.5 rounded-md text-sm font-medium transition-colors duration-150",
        isActive
          ? "bg-sidebar-accent text-sidebar-primary-foreground"
          : "text-sidebar-foreground/70 hover:text-sidebar-foreground hover:bg-sidebar-accent/50"
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
          <p className="text-xs text-muted-foreground font-normal">
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

  const currentPage =
    [...navItems, ...bottomNavItems].find(item => item.path === location) ||
    navItems[0];

  return (
    <div className="flex h-screen overflow-hidden">
      {/* Sidebar */}
      <aside
        className={cn(
          "flex flex-col bg-sidebar border-r border-sidebar-border transition-all duration-200 shrink-0",
          collapsed ? "w-16" : "w-60"
        )}
      >
        {/* Logo */}
        <div
          className={cn(
            "flex items-center gap-3 px-4 h-14 border-b border-sidebar-border shrink-0",
            collapsed && "justify-center px-2"
          )}
        >
          <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-sidebar-primary">
            <Shield className="h-4.5 w-4.5 text-sidebar-primary-foreground" />
          </div>
          {!collapsed && (
            <div className="flex flex-col min-w-0">
              <span className="text-sm font-bold text-sidebar-foreground tracking-tight truncate">
                Audit React
              </span>
              <span className="text-[10px] text-sidebar-foreground/50 font-medium tracking-wider uppercase">
                SEFIN
              </span>
            </div>
          )}
        </div>

        {/* Navigation */}
        <nav className="flex-1 flex flex-col px-2 py-3 gap-0.5 overflow-y-auto">
          {navItems.map(item => (
            <NavLink
              key={item.path}
              item={item}
              collapsed={collapsed}
              isActive={location === item.path}
            />
          ))}
        </nav>

        {/* Bottom nav */}
        <div className="px-2 pb-2 space-y-0.5">
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
            className="flex items-center gap-3 px-3 py-2 rounded-md text-sm text-sidebar-foreground/50 hover:text-sidebar-foreground hover:bg-sidebar-accent/50 transition-colors w-full"
            aria-expanded={!collapsed}
            aria-label={
              collapsed ? "Expandir menu lateral" : "Recolher menu lateral"
            }
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

      {/* Main content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Header */}
        <header className="flex items-center justify-between h-14 px-6 border-b border-border bg-background shrink-0">
          <div>
            <h1 className="text-base font-bold text-foreground tracking-tight">
              {currentPage.label}
            </h1>
            <p className="text-xs text-muted-foreground">
              {currentPage.description}
            </p>
          </div>
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2 px-3 py-1.5 rounded-md bg-muted text-xs">
              <span className="text-muted-foreground">CNPJ Ativo:</span>
              <span className="font-mono font-medium text-foreground">
                Nenhum selecionado
              </span>
            </div>
          </div>
        </header>

        {/* Page content */}
        <main className="flex-1 overflow-y-auto p-6">{children}</main>
      </div>
    </div>
  );
}
