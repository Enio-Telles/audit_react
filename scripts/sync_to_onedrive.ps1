# sync_to_onedrive.ps1
# Script para sincronizar os arquivos essenciais do projeto Fiscal Parquet para o OneDrive.

$ErrorActionPreference = "Stop"

# Configurações de Caminhos
$Source = "C:\Sistema_react"
$Destination = "C:\Users\03002693901\OneDrive - SECRETARIA DE ESTADO DE FINANCAS\Desenvolvimento\sist_react_02"

# Lista de Diretórios Essenciais
$Dirs = @(
    "src",
    "backend",
    "frontend",
    "sql",
    "docs",
    "dados\referencias",
    "scripts",
    "modelo",
    "tests"
)

# Lista de Arquivos de Raiz Essenciais
$RootFiles = @(
    ".env",
    ".gitignore",
    "requirements.txt",
    "AGENTS.md",
    "README.md",
    "FRONTEND.md",
    "app_react.py",
    "app.py"
)

Write-Host "--- Iniciando Sincronização para OneDrive ---" -ForegroundColor Cyan
Write-Host "Origem: $Source"
Write-Host "Destino: $Destination"

# 1. Sincronizar Arquivos de Raiz
Write-Host "`nSincronizando arquivos de raiz..." -ForegroundColor Yellow
foreach ($file in $RootFiles) {
    if (Test-Path "$Source\$file") {
        # O robocopy para um arquivo individual exige o diretório de origem e o nome do arquivo
        robocopy "$Source" "$Destination" "$file" /NDL /NFL /NJH /NJS /R:3 /W:5 /NP
    }
}

# 2. Sincronizar Diretórios
foreach ($dir in $Dirs) {
    Write-Host "Sincronizando $dir..." -ForegroundColor Yellow
    
    $Exclusions = ""
    if ($dir -eq "frontend") {
        $Exclusions = "/XD node_modules dist .next"
    } elseif ($dir -eq "src" -or $dir -eq "backend" -or $dir -eq "scripts" -or $dir -eq "tests") {
        $Exclusions = "/XD __pycache__"
    }

    # Executa robocopy com as exclusões apropriadas
    # /E: Subdiretórios (inclui vazios)
    # /R:3 /W:5: Retentativas em caso de erro (comum em OneDrive se arquivo estiver aberto)
    $cmd = "robocopy `"$Source\$dir`" `"$Destination\$dir`" /E /R:3 /W:5 /NDL /NFL /NJH /NJS $Exclusions /NP"
    Invoke-Expression $cmd
}

Write-Host "`n--- Sincronização Concluída com Sucesso ---" -ForegroundColor Green
