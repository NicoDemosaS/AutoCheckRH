#!/usr/bin/env python3
"""
Crawler simples para visitar os URLs lidos pelo leitor de QR e extrair informações.

Comportamento padrão:
- Lê `lidos.csv` (espera coluna `data` como segunda coluna)
- Para cada URL única tenta abrir via HTTP(S) (adiciona http:// se faltar esquema)
- Extrai: status_code, final_url, title, meta description, primeiro H1, número de links, emails encontrados, tempo de fetch
- Salva tudo em CSV (`resultados.csv` por padrão)

Uso:
    python3 crawler.py --input lidos.csv --output resultados.csv --workers 8

Observações:
- Respeite os sites que requerem robots.txt / login (este script não respeita robots e não faz login)
- Seja educado: diminua workers e aumente timeout se for fazer muitas requisições
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import csv
import os
import re
import time
from typing import List

import requests
from bs4 import BeautifulSoup


EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

# padrões para extrair campos de DANFE/NFC-e a partir do texto combinado
NUMERO_RE = re.compile(r"N[uú]mero\s*[:\u00A0]?\s*(\d+)", re.I)
EMISSAO_RE = re.compile(r"Emiss(?:ão|ao)\s*[:\u00A0]?\s*([0-9]{2}/[0-9]{2}/[0-9]{4}\s+[0-9]{2}:[0-9]{2}:[0-9]{2})", re.I)
VALOR_PAGAR_RE = re.compile(r"Valor\s*a\s*pagar\s*R\$\s*[:\u00A0]?\s*([0-9\.,]+)", re.I)


def normalize_url(u: str) -> str:
    u = u.strip()
    if not u:
        return u
    # preserve file URLs
    if u.startswith("file://"):
        return u
    if not u.startswith("http://") and not u.startswith("https://"):
        u = "http://" + u
    return u


def fetch_url(url: str, timeout: int = 10) -> dict:
    start = time.time()
    out = {
        "orig_url": url,
        "final_url": "",
        "numero": "",
        "emissao": "",
        "valor_pagar": "",
        "status_code": "",
        "title": "",
        "meta_description": "",
        "h1": "",
        "num_links": "",
        "emails": "",
        "fetch_time": "",
        "error": "",
    }

    if not url:
        out["error"] = "empty url"
        return out

    u = normalize_url(url)
    # suportar leitura de arquivos locais via file://
    try:
        if u.startswith("file://"):
            path = u[len("file://"):]
            if os.path.exists(path):
                out["status_code"] = 200
                out["final_url"] = u
                with open(path, "r", encoding="utf-8", errors="replace") as fh:
                    text = fh.read()
                soup = BeautifulSoup(text, "lxml")
            else:
                out["error"] = f"file not found: {path}"
                out["fetch_time"] = round(time.time() - start, 3)
                return out
        else:
            with requests.Session() as s:
                r = s.get(u, timeout=timeout, allow_redirects=True, headers={"User-Agent": "AutoCheckRH/1.0"})
                out["status_code"] = r.status_code
                out["final_url"] = r.url

                content_type = r.headers.get("content-type", "")
                text = r.text if "text" in content_type or "html" in content_type or content_type == "" else ""
                soup = BeautifulSoup(text, "lxml") if text else None

        if soup:
            title_tag = soup.title
            out["title"] = title_tag.string.strip() if title_tag and title_tag.string else ""

            meta = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
            if meta:
                out["meta_description"] = (meta.get("content") or "").strip()

            h1 = soup.find("h1")
            if h1:
                out["h1"] = h1.get_text(separator=" ", strip=True)

            links = soup.find_all("a")
            out["num_links"] = len(links)

            page_text = soup.get_text(separator=' ', strip=True)
            emails = set(EMAIL_RE.findall(page_text))
            out["emails"] = ",".join(sorted(emails))

            # extrair campos específicos de DANFE/NFC-e a partir do texto combinado
            m = NUMERO_RE.search(page_text)
            if m:
                out["numero"] = m.group(1)
            m2 = EMISSAO_RE.search(page_text)
            if m2:
                out["emissao"] = m2.group(1)

            # tentar extrair valor a pagar de span com classes ou por regex
            valor = ""
            # procurar span com classes que indicam total (ex.: totalNumb txtMax)
            for sp in soup.find_all('span', class_=True):
                classes = sp.get('class') or []
                if 'totalNumb' in classes and any('txtMax' in c for c in classes):
                    valor = sp.get_text(strip=True)
                    break
            if not valor:
                m3 = VALOR_PAGAR_RE.search(page_text)
                if m3:
                    valor = m3.group(1)
            out["valor_pagar"] = valor

    except Exception as e:
        out["error"] = str(e)
    finally:
        out["fetch_time"] = round(time.time() - start, 3)

    return out


def read_input_csv(path: str) -> List[str]:
    urls = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for row in reader:
            if not row:
                continue
            # assume data is in second column if exists, else first
            if len(row) >= 2:
                urls.append(row[1])
            else:
                urls.append(row[0])
    # deduplicate preserving order
    seen = set()
    uniq = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def write_results(path: str, rows: List[dict]):
    fieldnames = [
        "orig_url",
        "final_url",
        "numero",
        "emissao",
        "valor_pagar",
        "status_code",
        "title",
        "meta_description",
        "h1",
        "num_links",
        "emails",
        "fetch_time",
        "error",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main():
    parser = argparse.ArgumentParser(description="Crawler simples para URLs lidos de QR")
    parser.add_argument("--input", "-i", default="lidos.csv", help="CSV de entrada (padrão: lidos.csv)")
    parser.add_argument("--output", "-o", default="resultados.csv", help="CSV de saída (padrão: resultados.csv)")
    parser.add_argument("--workers", "-w", type=int, default=8, help="Número de threads concorrentes (padrão: 8)")
    parser.add_argument("--timeout", "-t", type=int, default=10, help="Timeout por requisição em segundos (padrão: 10)")
    args = parser.parse_args()

    urls = read_input_csv(args.input)
    if not urls:
        print("Nenhuma URL encontrada em", args.input)
        return

    print(f"Encontradas {len(urls)} URLs; iniciando fetch com {args.workers} workers...")

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(fetch_url, u, args.timeout): u for u in urls}
        for fut in as_completed(futures):
            u = futures[fut]
            try:
                res = fut.result()
            except Exception as e:
                res = {"orig_url": u, "error": str(e)}
            results.append(res)
            print(f"{res.get('orig_url')} -> status={res.get('status_code')} time={res.get('fetch_time')}s error={res.get('error')}")

    write_results(args.output, results)
    print("Resultado salvo em", args.output)


if __name__ == "__main__":
    main()
