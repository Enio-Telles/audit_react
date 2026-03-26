import { Toaster } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import NotFound from "@/pages/NotFound";
import { Route, Switch } from "wouter";
import ErrorBoundary from "./components/ErrorBoundary";
import { ThemeProvider } from "./contexts/ThemeContext";
import DashboardLayout from "./components/layout/DashboardLayout";
import Dashboard from "./pages/Dashboard";
import Extracao from "./pages/Extracao";
import Consulta from "./pages/Consulta";
import Agregacao from "./pages/Agregacao";
import Conversao from "./pages/Conversao";
import Estoque from "./pages/Estoque";
import Configuracoes from "./pages/Configuracoes";

function Router() {
  return (
    <DashboardLayout>
      <Switch>
        <Route path="/" component={Dashboard} />
        <Route path="/extracao" component={Extracao} />
        <Route path="/consulta" component={Consulta} />
        <Route path="/agregacao" component={Agregacao} />
        <Route path="/conversao" component={Conversao} />
        <Route path="/estoque" component={Estoque} />
        <Route path="/configuracoes" component={Configuracoes} />
        <Route path="/404" component={NotFound} />
        <Route component={NotFound} />
      </Switch>
    </DashboardLayout>
  );
}

function App() {
  return (
    <ErrorBoundary>
      <ThemeProvider defaultTheme="light">
        <TooltipProvider>
          <Toaster />
          <Router />
        </TooltipProvider>
      </ThemeProvider>
    </ErrorBoundary>
  );
}

export default App;
