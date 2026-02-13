#!/usr/bin/env python3
import json
import os
from pathlib import Path

import urllib.parse
import requests
import pandas as pd
from bs4 import BeautifulSoup


def dict_agua_embalses(fail_silently: bool = True):
    """
    Devuelve dict_agua o None si no se puede parsear.
    Si fail_silently=False, lanza RuntimeError.
    """
    url = "https://www.embalses.net/"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.google.com/",
    }

    targets = [
        ("allorigins", "https://api.allorigins.win/raw?url=" + urllib.parse.quote_plus(url)),
        ("direct", url),
        ("jina", "https://r.jina.ai/http://www.embalses.net/"),
    ]

    def _parse(html: str):
        soup = BeautifulSoup(html, "lxml")

        seccion = soup.find("div", class_="SeccionCentral")
        filas = seccion.find_all("div", class_="FilaSeccion") if seccion else soup.find_all("div", class_="FilaSeccion")
        if not filas:
            return None

        lista = ['AGUA_TOTAL', 'VARIACION', 'CAPACIDAD', 'MISMA SEMANA(t-1)', 'MISMA SEMANA (t-10)']
        dict_agua = {}

        for k, row in enumerate(filas[:len(lista)]):
            key = lista[k]
            dict_agua[key] = {}

            campo_div = row.find("div", class_="Campo")
            if campo_div is None:
                continue
            campo_txt = campo_div.get_text(strip=True)

            if key == 'AGUA_TOTAL':
                if "(" in campo_txt and ")" in campo_txt:
                    campo_raw = campo_txt.split("(")[1].split(")")[0].strip()
                else:
                    campo_raw = campo_txt.strip()
                campo = pd.to_datetime(campo_raw, format="%d-%m-%Y", errors="coerce")
                # json no serializa Timestamp bien -> convertir a ISO si existe
                campo = campo.date().isoformat() if pd.notna(campo) else None
            else:
                campo = campo_txt

            dict_agua[key]["campo"] = campo

            resultados = row.find_all("div", class_="Resultado")
            if not resultados:
                continue

            for l, r in enumerate(resultados):
                txt = r.get_text(strip=True)
                txt_norm = txt.replace(",", ".")
                if txt.count(".") >= 1 and "," in txt:
                    txt_norm = txt.replace(".", "").replace(",", ".")

                if l == 0:
                    if key != "VARIACION":
                        dict_agua[key]["resultado1"] = float(txt_norm) * 1000
                    else:
                        dict_agua[key]["resultado1"] = float(txt_norm)
                else:
                    dict_agua[key]["resultado2"] = float(txt_norm)

        return dict_agua

    last_debug = {}
    for name, target in targets:
        try:
            print(f"[DEBUG dict_agua_embalses] intentando {name}: {target}")
            r = requests.get(target, timeout=30, headers=headers)
            html = r.text or ""
            print(f"[DEBUG dict_agua_embalses] {name} status={r.status_code} len={len(html)}")

            last_debug = {
                "source": name,
                "status": r.status_code,
                "len": len(html),
                "title": None,
                "markers": None,
                "snippet": html[:800],
            }

            d = _parse(html)
            if d is not None and "AGUA_TOTAL" in d:
                print(f"[DEBUG dict_agua_embalses] OK parseado desde {name}")
                return d

            soup = BeautifulSoup(html, "lxml")
            last_debug["title"] = soup.title.text.strip() if soup.title else None
            markers = ["captcha", "cloudflare", "access denied", "forbidden", "bot", "error", "blocked"]
            last_debug["markers"] = [m for m in markers if m in html.lower()]
            print(f"[DEBUG dict_agua_embalses] {name} no encontró estructura. title={last_debug['title']} markers={last_debug['markers']}")

        except Exception as e:
            print(f"[DEBUG dict_agua_embalses] {name} ERROR: {repr(e)}")

    msg = (
        "No se pudo parsear embalses.net: estructura no encontrada (FilaSeccion/SeccionCentral). "
        f"Último intento: {last_debug.get('source')} status={last_debug.get('status')} len={last_debug.get('len')} "
        f"title={last_debug.get('title')} markers={last_debug.get('markers')}"
    )
    print("[DEBUG dict_agua_embalses] FAIL:", msg)
    print("[DEBUG dict_agua_embalses] snippet:\n", last_debug.get("snippet"))

    if fail_silently:
        return None
    raise RuntimeError(msg)


if __name__ == "__main__":
    data = dict_agua_embalses(fail_silently=False)

    out_dir = Path("data")
    out_dir.mkdir(exist_ok=True)

    out_path = out_dir / "embalses.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"[OK] Guardado {out_path}")
