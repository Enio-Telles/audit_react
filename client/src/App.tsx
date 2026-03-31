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
import DadosCadastrais from "./pages/DadosCadastrais";
import Dashboard from "./pages/Dashboard";
import Extracao from "./pages/Extracao";
import Consulta from "./pages/Consulta";
import PainelAgregacaoConversao from "./pages/PainelAgregacaoConversao";
import Estoque from "./pages/Estoque";
import Configuracoes from "./pages/Configuracoes";
import Relatorios from "./pages/Relatorios";
import MapeamentoOracle from "./pages/MapeamentoOracle";

function PainelRouter() {
  return (
    <DashboardLayout>
      <Switch>
        <Route path="/dashboard" component={Dashboard} />
        <Route path="/dados-cadastrais" component={DadosCadastrais} />
        <Route path="/extracao" component={Extracao} />
        <Route path="/consulta" component={Consulta} />
        <Route
          path="/agregacao-conversao"
          component={PainelAgregacaoConversao}
        />
        <Route path="/estoque" component={Estoque} />
        <Route path="/mapeamento-oracle" component={MapeamentoOracle} />
        <Route path="/configuracoes" component={Configuracoes} />
        <Route path="/relatorios" component={Relatorios} />
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
