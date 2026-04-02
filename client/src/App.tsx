import { Toaster } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import NotFound from "@/pages/NotFound";
import { Route, Switch } from "wouter";
import ErrorBoundary from "./components/ErrorBoundary";
import { ThemeProvider } from "./contexts/ThemeContext";
import { CnpjProvider } from "./contexts/CnpjContext";
import { SelecaoOperacionalProvider } from "./contexts/SelecaoOperacionalContext";
import DashboardLayout from "./components/layout/DashboardLayout";
import Home from "./pages/Home";
import Consulta from "./pages/Consulta";
import PainelAgregacaoConversao from "./pages/PainelAgregacaoConversao";
import Estoque from "./pages/Estoque";
import Configuracoes from "./pages/Configuracoes";

function PainelRouter() {
  return (
    <DashboardLayout>
      <Switch>
        <Route path="/consulta" component={Consulta} />
        <Route
          path="/agregacao-conversao"
          component={PainelAgregacaoConversao}
        />
        <Route path="/estoque" component={Estoque} />
        <Route path="/configuracoes" component={Configuracoes} />
        <Route path="/404" component={NotFound} />
        <Route component={NotFound} />
      </Switch>
    </DashboardLayout>
  );
}

function Router() {
  return (
    <Switch>
      <Route path="/" component={Home} />
      <Route component={PainelRouter} />
    </Switch>
  );
}

function App() {
  return (
    <ErrorBoundary>
      <ThemeProvider defaultTheme="light">
        <CnpjProvider>
          <SelecaoOperacionalProvider>
            <TooltipProvider>
              <Toaster />
              <Router />
            </TooltipProvider>
          </SelecaoOperacionalProvider>
        </CnpjProvider>
      </ThemeProvider>
    </ErrorBoundary>
  );
}

export default App;
