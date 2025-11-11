#!/usr/bin/env python3
"""Gerar planilha feita comparando `comparardados.csv` com um `resultados-clean-*.csv`.

Comportamento:
- Lê os dois arquivos (padrões: `comparardados.csv` e `resultados-clean-YYYYMMDD-HHMMSS.csv`)
- Para cada linha de `comparardados` procura por `numNotaFiscal` nos `numero` de `resultados-clean`.
- Normaliza números removendo espaços e zeros à esquerda; quando não encontrar, tenta achar o número mais próximo (diferença <= 5)
- Quando achar um match usa o `valor_pagar` e `emissao` do resultado para classificar:
  - valor entre 40 e 55 (inclusive) e horário antes das 16:00 -> ALMOCO
  - valor entre 40 e 55 (inclusive) e horário >=16:00 -> JANTA
  - valor > 100 -> HOTEL
  - se não encontrar -> NAO_ENCONTRADO
- Gera um CSV timestamped `planilhaFeita-YYYYMMDD-HHMMSS.csv` com as colunas originais de `comparardados` mais as colunas `classificacao, matched_num, matched_valor, matched_emissao, observacao`

O script é verboso e imprime passo a passo o que encontra; também escreve o CSV final.

Uso:
    python make_planilha.py --comparar comparardados.csv --resultados resultados-clean-20251105-183405.csv

"""
import argparse
import csv
import datetime
import os
import re
import time
from typing import Optional, Tuple, List, Dict


def parse_brazil_currency(s: str) -> Optional[float]:
    if s is None:
        return None
    s = str(s).strip().replace('"', '').replace(' ', '')
    if s == '':
        return None
    # remove thousands separators and convert comma decimal to dot
    s = s.replace('.', '')
    s = s.replace(',', '.')
    try:
        return float(s)
    except Exception:
        return None


def parse_result_emissao(s: str) -> Optional[datetime.datetime]:
    if not s:
        return None
    s = s.strip().replace('"', '')
    # expected format: DD/MM/YYYY HH:MM:SS or DD/MM/YYYY
    for fmt in ('%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M', '%d/%m/%Y'):
        try:
            return datetime.datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def normalize_num_for_compare(s: str) -> Optional[int]:
    if s is None:
        return None
    s = str(s).strip()
    if s == '':
        return None
    # capture first run of digits
    m = re.search(r"(\d+)", s)
    if not m:
        return None
    num = m.group(1)
    # remove leading zeros to normalize
    num = num.lstrip('0') or '0'
    try:
        return int(num)
    except Exception:
        return None


def load_resultados(path: str) -> List[Dict]:
    rows = []
    # use utf-8-sig to safely strip BOM if present and sniff delimiter
    with open(path, newline='', encoding='utf-8-sig') as f:
        sample = f.read(4096)
        if not sample:
            return rows
        try:
            dialect = csv.Sniffer().sniff(sample)
            delim = getattr(dialect, 'delimiter', ',')
            if delim not in (',', ';', '\t', '|'):
                delim = ','
        except Exception:
            delim = ','
        f.seek(0)
        reader = csv.DictReader(f, delimiter=delim, skipinitialspace=True)
        for r in reader:
            # skip empty rows
            if not any((v and str(v).strip()) for v in r.values()):
                continue
            numero_raw = r.get('numero', '')
            numero = normalize_num_for_compare(numero_raw)
            valor = parse_brazil_currency(r.get('valor_pagar', ''))
            emissao = parse_result_emissao(r.get('emissao', ''))
            rows.append({
                'numero_raw': numero_raw,
                'numero': numero,
                'valor': valor,
                'emissao': emissao,
                'orig': r,
            })
    return rows


def load_comparar(path: str) -> List[Dict]:
    rows = []
    # robust reading: handle BOM, detect delimiter and skip empty rows
    with open(path, newline='', encoding='utf-8-sig') as f:
        sample = f.read(4096)
        if not sample:
            return rows
        try:
            dialect = csv.Sniffer().sniff(sample)
            delim = getattr(dialect, 'delimiter', ',')
            if delim not in (',', ';', '\t', '|'):
                delim = ','
        except Exception:
            delim = ','
        f.seek(0)
        reader = csv.DictReader(f, delimiter=delim, skipinitialspace=True)
        for r in reader:
            if not any((v and str(v).strip()) for v in r.values()):
                continue
            rows.append(r)
    return rows


def find_best_match(target_num: Optional[int], resultados: List[Dict], max_diff=5) -> Tuple[Optional[Dict], Optional[int]]:
    """Return (best_row, diff) if found, else (None, None)."""
    if target_num is None:
        return None, None
    # exact match first
    for row in resultados:
        if row['numero'] is not None and row['numero'] == target_num:
            return row, 0

    # find nearest numeric by absolute difference
    best = None
    best_diff = None
    for row in resultados:
        if row['numero'] is None:
            continue
        diff = abs(row['numero'] - target_num)
        if best is None or diff < best_diff:
            best = row
            best_diff = diff

    if best is not None and best_diff is not None and best_diff <= max_diff:
        return best, best_diff
    return None, None


def classify_by_valor_emissao(valor: Optional[float], emissao: Optional[datetime.datetime]) -> str:
    if valor is None:
        return 'SEM_VALOR'
    if valor > 100:
        return 'HOTEL'
    if 40 <= valor <= 55:
        # decide by hora de emissao
        if emissao is None:
            return 'ALMOCO?'  # unknown horario
        if emissao.hour < 16:
            return 'ALMOCO'
        else:
            return 'JANTA'
    return 'OUTRO'


def main():
    p = argparse.ArgumentParser(description='Comparar notas e gerar planilha feita')
    p.add_argument('--comparar', default='comparardados.csv')
    p.add_argument('--resultados', default='resultados-clean-20251105-183405.csv')
    p.add_argument('--out', default='planilhaFeita')
    args = p.parse_args()

    print(f'Carregando resultados de: {args.resultados}')
    resultados = load_resultados(args.resultados)
    print(f'Linhas de resultados carregadas: {len(resultados)}')

    print(f'Carregando comparador de: {args.comparar}')
    comps = load_comparar(args.comparar)
    print(f'Linhas em comparardados: {len(comps)}')

    # prepare output rows (will include original comparar columns)
    out_rows = []

    for idx, comp in enumerate(comps, start=1):
        orig_num = comp.get('numNotaFiscal') or comp.get('numNotaFiscal'.lower()) or ''
        print('\n---')
        print(f'[{idx}] numNotaFiscal original: "{orig_num}"')
        target = normalize_num_for_compare(orig_num)
        print(f'    Normalizado para comparação: {target}')

        matched_row, diff = find_best_match(target, resultados)
        note = ''
        matched_num = ''
        matched_valor = ''
        matched_emissao = ''
        classificacao = ''

        if matched_row is None:
            note = 'NAO_ENCONTRADO'
            classificacao = 'NAO_ENCONTRADO'
            print('    -> Não encontrado nenhum número correspondente nos resultados-clean')
        else:
            matched_num = str(matched_row.get('numero') if matched_row.get('numero') is not None else '')
            matched_val = matched_row.get('valor')
            matched_em = matched_row.get('emissao')
            matched_valor = f'{matched_val:.2f}' if matched_val is not None else ''
            matched_emissao = matched_em.isoformat(sep=' ') if matched_em is not None else ''
            if diff == 0:
                note = 'OK'
                print(f'    -> Encontrado match exato: {matched_num} (valor={matched_valor}, emissao={matched_emissao})')
            else:
                note = f'VERIFICAR_NUMNOTA (dif={diff}, candidato={matched_num})'
                print(f'    -> Encontrado match aproximado: {matched_num} (dif={diff})')

            # classify using matched result values
            classificacao = classify_by_valor_emissao(matched_val, matched_em)
            print(f'    -> Classificação baseada em valor/emissão do resultado: {classificacao}')

            # additionally compare values from comparardados if present
            comp_val_raw = comp.get('Valor') or comp.get('valor') or ''
            comp_val = parse_brazil_currency(comp_val_raw)
            if comp_val is not None and matched_val is not None:
                if abs(comp_val - matched_val) > 0.5:
                    note += f' | VAL_DIFF (comp={comp_val:.2f} vs res={matched_val:.2f})'
                    print(f'    -> Atenção: diferença de valores entre comparardados ({comp_val:.2f}) e resultados ({matched_val:.2f})')

            # compare dates (day-month-year)
            comp_date_raw = comp.get('Data') or comp.get('data') or ''
            comp_date = None
            if comp_date_raw:
                try:
                    comp_date = datetime.datetime.strptime(comp_date_raw.strip(), '%d-%m-%y').date()
                except Exception:
                    try:
                        comp_date = datetime.datetime.strptime(comp_date_raw.strip(), '%d-%m-%Y').date()
                    except Exception:
                        comp_date = None
            if comp_date and matched_em is not None:
                if comp_date != matched_em.date():
                    note += f' | DATE_MISMATCH (comp={comp_date} vs res={matched_em.date()})'
                    print(f'    -> Atenção: data diferente (comparardados: {comp_date} != resultados: {matched_em.date()})')

        out_row = dict(comp)  # copy all original fields
        out_row.update({
            'classificacao': classificacao,
            'matched_num': matched_num,
            'matched_valor': matched_valor,
            'matched_emissao': matched_emissao,
            'observacao': note,
        })
        out_rows.append(out_row)

    # write output CSV
    ts = time.strftime('%Y%m%d-%H%M%S')
    out_name = f"{args.out}-{ts}.csv"
    # determine header: original comparar header + our added columns
    if comps:
        header = list(comps[0].keys())
    else:
        header = ['numNotaFiscal', 'Data', 'Valor']
    extra = ['classificacao', 'matched_num', 'matched_valor', 'matched_emissao', 'observacao']
    header.extend(extra)

    with open(out_name, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for r in out_rows:
            # ensure all header keys present
            row = {k: (r.get(k, '') if r.get(k, '') is not None else '') for k in header}
            writer.writerow(row)

    print('\nProcessamento concluído.')
    print(f'Planilha gerada: {out_name}')


if __name__ == '__main__':
    main()
