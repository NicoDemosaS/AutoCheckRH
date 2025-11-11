#!/usr/bin/env python3
"""Comparar planilhas: lê `inputs/comparar.csv` e um `outputs/planilha_feita-<N>.csv` e gera
um arquivo `outputs/resultados-<M>.csv` numerado sequencialmente.

Uso:
    python compare_planilha.py --comparar inputs/comparar.csv --planilha outputs/planilha_feita-1.csv
"""
import argparse
import csv
import datetime
import os
import re
import time
from typing import List, Dict, Optional, Tuple

BASE_INPUT_DIR = os.path.join(os.path.dirname(__file__), 'inputs')
BASE_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'outputs')


def next_seq_file(directory: str, prefix: str, ext: str) -> str:
    os.makedirs(directory, exist_ok=True)
    existing = os.listdir(directory)
    pat = re.compile(re.escape(prefix) + r"-(\d+)" + re.escape(ext) + r"$")
    nums = [int(m.group(1)) for name in existing if (m := pat.search(name))]
    n = max(nums) + 1 if nums else 1
    return os.path.join(directory, f"{prefix}-{n}{ext}")


def parse_currency(s: str) -> Optional[float]:
    if s is None:
        return None
    s = str(s).strip().replace('"','').replace(' ','')
    if s == '':
        return None
    s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except Exception:
        return None


def parse_date(s: str) -> Optional[datetime.date]:
    if not s:
        return None
    for fmt in ('%d-%m-%y','%d-%m-%Y'):
        try:
            return datetime.datetime.strptime(s.strip(), fmt).date()
        except Exception:
            continue
    return None


def load_csv_rows(path: str) -> List[Dict]:
    rows = []
    with open(path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        for r in reader:
            if not any((v and str(v).strip()) for v in r.values()):
                continue
            rows.append(r)
    return rows


def normalize_num(s: str) -> Optional[int]:
    if s is None:
        return None
    m = re.search(r"(\d+)", str(s))
    if not m:
        return None
    v = m.group(1).lstrip('0') or '0'
    return int(v)


def find_best(target: Optional[int], resultados: List[Dict], maxdiff=5) -> Tuple[Optional[Dict], Optional[int]]:
    if target is None:
        return None, None
    for r in resultados:
        if normalize_num(r.get('numero')) == target:
            return r, 0
    best = None; bd = None
    for r in resultados:
        num = normalize_num(r.get('numero'))
        if num is None:
            continue
        diff = abs(num - target)
        if best is None or diff < bd:
            best = r; bd = diff
    if bd is not None and bd <= maxdiff:
        return best, bd
    return None, None


def classify(valor: Optional[float], emissao_str: str) -> str:
    if valor is None:
        return 'SEM_VALOR'
    if valor > 100:
        return 'HOTEL'
    if 40 <= valor <= 55:
        # parse hour
        try:
            em = datetime.datetime.strptime(emissao_str.split()[0], '%d/%m/%Y') if '/' in emissao_str else None
        except Exception:
            em = None
        # fallback: if time present in emissao_str
        hour = None
        m = re.search(r"(\d{2}):(\d{2})", emissao_str)
        if m:
            hour = int(m.group(1))
        if hour is None:
            return 'ALMOCO?'
        return 'ALMOCO' if hour < 16 else 'JANTA'
    return 'OUTRO'


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--comparar', default=os.path.join('inputs','comparar.csv'))
    p.add_argument('--planilha', default=os.path.join('outputs','planilha_feita-1.csv'))
    args = p.parse_args()

    if not os.path.exists(args.comparar):
        print('comparar file not found:', args.comparar); return
    if not os.path.exists(args.planilha):
        print('planilha file not found:', args.planilha); return

    comps = load_csv_rows(args.comparar)
    resultados = load_csv_rows(args.planilha)
    print(f'Loaded {len(comps)} comparar rows and {len(resultados)} planilha rows')

    out_rows = []
    for c in comps:
        num_raw = c.get('numNotaFiscal')
        target = normalize_num(num_raw)
        matched, diff = find_best(target, resultados)
        matched_num = ''
        matched_val = ''
        matched_em = ''
        note = ''
        classification = ''
        if matched is None:
            note = 'NAO_ENCONTRADO'; classification = 'NAO_ENCONTRADO'
        else:
            matched_num = str(normalize_num(matched.get('numero')) or '')
            matched_val = parse_currency(matched.get('valor_pagar'))
            matched_em = matched.get('emissao','')
            classification = classify(matched_val, matched_em)
            note = 'OK' if diff == 0 else f'VERIFICAR_NUMNOTA (dif={diff})'
        out = dict(c)
        out.update({'classificacao': classification, 'matched_num': matched_num, 'matched_valor': matched_val, 'matched_emissao': matched_em, 'observacao': note})
        out_rows.append(out)

    out_path = next_seq_file(BASE_OUTPUT_DIR, 'resultados', '.csv')
    # header
    header = list(comps[0].keys()) if comps else ['numNotaFiscal','Data','Valor']
    header += ['classificacao','matched_num','matched_valor','matched_emissao','observacao']
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in out_rows:
            w.writerow({k: r.get(k,'') for k in header})

    print('Wrote', out_path)


if __name__ == '__main__':
    main()
