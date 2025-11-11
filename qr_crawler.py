#!/usr/bin/env python3
"""QR crawler: lê URLs de `inputs/qr_links.csv`, busca as páginas e gera
uma planilha limpa em `outputs/planilha_feita-<N>.csv` contendo apenas
numero,emissao,valor_pagar, e um log em `logs/qr_crawler-<N>.log`.

Uso:
    python qr_crawler.py --input inputs/qr_links.csv --workers 4 --delay 0.5

Observação: os arquivos de saída são sequenciais (1,2,3...) para facilitar testes.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import csv
import os
import re
import time
import threading
from typing import List
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup


BASE_INPUT_DIR = os.path.join(os.path.dirname(__file__), 'inputs')
BASE_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'outputs')
BASE_LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')


def next_seq_file(directory: str, prefix: str, ext: str) -> str:
    """Return path with next sequence number: e.g. prefix-1.ext, prefix-2.ext."""
    os.makedirs(directory, exist_ok=True)
    existing = os.listdir(directory)
    pat = re.compile(re.escape(prefix) + r"-(\d+)" + re.escape(ext) + r"$")
    nums = [int(m.group(1)) for name in existing if (m := pat.search(name))]
    n = max(nums) + 1 if nums else 1
    return os.path.join(directory, f"{prefix}-{n}{ext}")


EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

NUMERO_RE = re.compile(r"(\d{3,44})")
EMISSAO_RE = re.compile(r"(\d{2}/\d{2}/\d{4}(?:\s+\d{2}:\d{2}(?::\d{2})?)?)")
VALOR_RE = re.compile(r"R\$\s*([0-9\.,]+)|([0-9]{1,3}(?:[.,][0-9]{2}))")

# simple per-host throttling
_LAST_REQUEST = {}
_LAST_REQUEST_LOCK = threading.Lock()


def normalize_url(u: str) -> str:
    u = (u or '').strip()
    for ch in ("\ufeff", "\u200b", "\u00A0"):
        u = u.replace(ch, '')
    if not u:
        return u
    low = u.lower()
    for scheme in ('http://', 'https://', 'file://'):
        idx = low.find(scheme)
        if idx > 0:
            u = u[idx:]
            break
    if u.startswith('file://'):
        return u
    if not u.startswith('http://') and not u.startswith('https://'):
        u = 'http://' + u
    return u


def fetch_url(url: str, timeout: int = 10, per_host_delay: float = 0.0) -> dict:
    start = time.time()
    out = {
        'orig_url': url,
        'final_url': '',
        'numero': '',
        'emissao': '',
        'valor_pagar': '',
        'status_code': '',
        'title': '',
        'emails': '',
        'fetch_time': '',
        'error': '',
    }
    if not url:
        out['error'] = 'empty url'
        return out
    u = normalize_url(url)
    try:
        if u.startswith('file://'):
            path = u[len('file://'):]
            if os.path.exists(path):
                out['status_code'] = 200
                out['final_url'] = u
                with open(path, 'r', encoding='utf-8', errors='replace') as fh:
                    text = fh.read()
                soup = BeautifulSoup(text, 'lxml')
            else:
                out['error'] = f'file not found: {path}'
                return out
        else:
            if per_host_delay and per_host_delay > 0:
                try:
                    host = urlparse(u).hostname or u
                except Exception:
                    host = u
                with _LAST_REQUEST_LOCK:
                    last = _LAST_REQUEST.get(host, 0)
                    allowed = last + per_host_delay
                    now = time.time()
                    wait = max(0, allowed - now)
                    _LAST_REQUEST[host] = now + wait
                if wait > 0:
                    time.sleep(wait)

            with requests.Session() as s:
                r = s.get(u, timeout=timeout, allow_redirects=True, headers={'User-Agent': 'QR-Crawler/1.0'})
                out['status_code'] = r.status_code
                out['final_url'] = r.url
                text = r.text if 'text' in r.headers.get('content-type','') or 'html' in r.headers.get('content-type','') else ''
                soup = BeautifulSoup(text, 'lxml') if text else None

        if soup:
            out['title'] = (soup.title.string.strip() if soup.title and soup.title.string else '')
            page_text = soup.get_text(' ', strip=True)
            emails = set(EMAIL_RE.findall(page_text))
            out['emails'] = ','.join(sorted(emails))

            # try find numero/emissao/valor with permissive patterns
            mnum = NUMERO_RE.search(page_text)
            if mnum:
                out['numero'] = mnum.group(1)
            mem = EMISSAO_RE.search(page_text)
            if mem:
                out['emissao'] = mem.group(1)
            mval = VALOR_RE.search(page_text)
            if mval:
                out['valor_pagar'] = (mval.group(1) or mval.group(2) or '').strip()

    except Exception as e:
        out['error'] = str(e)
    finally:
        out['fetch_time'] = round(time.time() - start, 3)
    return out


def read_input_csv(path: str) -> List[str]:
    urls = []
    with open(path, newline='', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for row in reader:
            if not row:
                continue
            val = row[1] if len(row) >= 2 else row[0]
            if val is None:
                continue
            for ch in ('\ufeff','\u200b','\u00A0'):
                val = val.replace(ch, '')
            val = val.strip()
            if not val:
                continue
            urls.append(val)
    # deduplicate preserving order
    seen = set(); uniq = []
    for u in urls:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq


def write_outputs_seq(base_name: str, rows: List[dict]):
    # create sequential filenames
    clean_path = next_seq_file(BASE_OUTPUT_DIR, base_name, '.csv')
    log_path = next_seq_file(BASE_LOG_DIR, base_name + '_log', '.log')
    # clean CSV
    with open(clean_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['numero','emissao','valor_pagar'])
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k,'') for k in ['numero','emissao','valor_pagar']})
    # log CSV
    with open(log_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['orig_url','final_url','emails','fetch_time','error','title','status_code'])
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k,'') for k in ['orig_url','final_url','emails','fetch_time','error','title','status_code']})
    return clean_path, log_path


def main():
    parser = argparse.ArgumentParser(description='QR crawler (new organized layout)')
    parser.add_argument('--input','-i', default=os.path.join('inputs','qr_links.csv'))
    parser.add_argument('--workers','-w', type=int, default=6)
    parser.add_argument('--timeout','-t', type=int, default=10)
    parser.add_argument('--delay','-d', type=float, default=0.5)
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print('Input file not found:', args.input)
        return

    urls = read_input_csv(args.input)
    if not urls:
        print('No URLs found in', args.input)
        return

    print(f'Found {len(urls)} URLs; fetching with {args.workers} workers (per-host delay {args.delay}s)')
    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(fetch_url, u, args.timeout, args.delay): u for u in urls}
        for fut in as_completed(futures):
            u = futures[fut]
            try:
                res = fut.result()
            except Exception as e:
                res = {'orig_url': u, 'error': str(e)}
            results.append(res)
            print(f"{res.get('orig_url')} -> status={res.get('status_code')} time={res.get('fetch_time')}s error={res.get('error')}")

    clean_path, log_path = write_outputs_seq('planilha_feita', results)
    print('Planilha limpa:', clean_path)
    print('Log:', log_path)


if __name__ == '__main__':
    main()
