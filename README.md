
# AutoCheckRH
Automação em Python para leitura de QR Codes e validação de dados do RH, desenvolvida para otimizar tarefas semanais manuais no setor de saúde.

## Visão geral

Fluxo de uso básico:

1. Executar `main.py` para capturar/ler QR codes e gerar uma lista de links.
2. Executar `qr_crawler.py` para visitar cada link lido, extrair os campos principais (numero, emissao, valor_pagar) e gerar uma planilha "limpa" em `outputs/` e um arquivo de log em `logs/`.
3. Executar `compare_planilha.py` (ou `make_planilha.py` em versões antigas) para comparar a sua planilha de referência (`inputs/comparar.csv`) com a planilha gerada pelo crawler e produzir `outputs/resultados-<N>.csv` com classificações.

> Estrutura de diretórios importante

- `inputs/`  — coloque aqui `qr_links.csv` (lista de links QR), `comparar.csv` (sua planilha de referência) e outros arquivos de entrada.
- `outputs/` — planilhas geradas, por exemplo `planilha_feita-1.csv`, `resultados-1.csv`.
- `logs/`    — logs de execução, por exemplo `planilha_feita_log-1.log`.

## Requisitos

O projeto usa Python 3 e as seguintes dependências (veja `requirements.txt`):

- requests
- beautifulsoup4

Instalação rápida (PowerShell):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Passo a passo de uso

1) Ler QR codes e gerar lista de links

Execute `main.py`. Ele deve salvar ou atualizar `inputs/qr_links.csv` com os links lidos.

Exemplo (PowerShell):

```powershell
cd C:\Users\Administrador\Desktop\AutoCheckRH-main
python .\main.py
```

2) Rodar o crawler sobre os links

`qr_crawler.py` percorre cada URL em `inputs/qr_links.csv` e tenta extrair `numero`, `emissao` e `valor_pagar`. O script grava:

- `outputs/planilha_feita-<N>.csv` — arquivo "limpo" contendo apenas as colunas `numero,emissao,valor_pagar`.
- `logs/planilha_feita_log-<N>.log` — log detalhado por linha com erros e metadados.

Exemplo (PowerShell):

```powershell
python .\qr_crawler.py --delay 1.0
```

Opções úteis:
- `--delay` (float) — atraso por host em segundos entre requisições para diminuir taxa e evitar bloqueios.

3) Comparar planilhas

Use `compare_planilha.py` para comparar a sua planilha de referência (`inputs/comparar.csv`) com a planilha gerada pelo crawler (`outputs/planilha_feita-<N>.csv`). O script produz `outputs/resultados-<M>.csv` com uma coluna de classificação (por exemplo: `ALMOCO`, `JANTA`, `HOTEL`, `NAO_ENCONTRADO`).

Exemplo (PowerShell):

```powershell
python .\compare_planilha.py inputs\comparar.csv outputs\planilha_feita-1.csv
```

Se preferir, você também pode passar apenas o nome do arquivo gerado pelo crawler:

```powershell
python .\compare_planilha.py inputs\comparar.csv
```

O script tentará localizar automaticamente o arquivo `planilha_feita-<N>.csv` mais recente em `outputs/` caso não seja informado.

## Saída esperada

- `outputs/planilha_feita-<N>.csv` — planilha limpa com as colunas: `numero,emissao,valor_pagar`.
- `logs/planilha_feita_log-<N>.log` — log detalhado com URLs, erros e tempos.
- `outputs/resultados-<M>.csv` — resultado da comparação, com colunas adicionais (classificação e qualquer informação encontrada).

## Dicas e solução de problemas

- Se `qr_crawler.py` falhar ao extrair valores em muitos URLs, experimenta aumentar o timeout e adicionar retries; algumas páginas exigem rendering JS e talvez seja preciso usar Selenium/Playwright.
- Se receber `PermissionError` ao salvar resultados, verifique se algum editor (por exemplo, OnlyOffice) segurou o arquivo (`.~lock.*` dentro da pasta). Feche o editor ou remova o arquivo de lock.
- Se `git push` falhar com "src refspec main does not match any", provavelmente seu repositório local não tinha commits ou o branch local ainda era `master`. Neste projeto executei os passos necessários para criar o commit inicial, renomear para `main` e fazer push. Ajuste `user.name` e `user.email` se necessário:

```powershell
git config --global user.name "Seu Nome"
git config --global user.email "seu@exemplo.com"
```

## Como contribuir

- Crie um branch baseado em `main`, faça alterações e abra um Pull Request.
- Por favor, evite commitar arquivos grandes de resultados finais; coloque-os em `.gitignore` se gerados localmente.

## Licença

Veja o arquivo `LICENSE` no repositório.
