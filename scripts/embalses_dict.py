#!/usr/bin/env python3
# scripts/embalses_dict.py

import json
from pathlib import Path

import requests
import pandas as pd
from bs4 import BeautifulSoup

# (Opcional) silenciar warning por verify=False
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _to_float_maybe(txt: str):
    """
    Convierte textos tipo:
      - '12,3' -> 12.3
      - '12.345,6' -> 12345.6
      - '5.634' (miles con punto en ES) -> 5634.0
    Devuelve None si viene vacío o no es parseable.
    """
    if txt is None:
        return None
    txt = txt.strip()
    if not txt:
        return None

    # Si hay coma, asumimos formato ES decimal con coma (y posible miles con punto)
    # 12.345,6 -> 12345.6
    if "," in txt:
        txt = txt.replace(".", "").replace(",", ".")
        try:
            return float(txt)
        except ValueError:
            return None

    # Si NO hay coma pero hay punto:
    # embalses a veces usa el punto como separador de miles en "Variación" (p.ej. 5.634)
    # Heurística: si tiene 1 punto y exactamente 3 dígitos detrás -> miles
    if txt.count(".") == 1:
        left, right = txt.split(".")
        if right.isdigit() and len(right) == 3 and left.replace("-", "").isdigit():
            txt = left + right  # 5.634 -> 5634
            try:
                return float(txt)
            except ValueError:
                return None

    # Caso general: intentar directo
    try:
        return float(txt)
    except ValueError:
        return None


def dict_agua_embalses(fail_silently: bool = True):
    """
    Devuelve dict_agua o None si no se puede parsear.
    Si fail_silently=False, lanza excepción.
    """
    url = "https://www.embalses.net/"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    }

    try:
        r = requests.get(url, headers=headers, timeout=30, verify=False)
        r.raise_for_status()
    except Exception as e:
        if fail_silently:
            print(f"[embalses] ERROR request: {e!r}")
            return None
        raise

    # parser lxml si está instalado; si no, usa html.parser
    try:
        soup = BeautifulSoup(r.content, "lxml")
    except Exception:
        soup = BeautifulSoup(r.content, "html.parser")

    seccion = soup.find("div", class_="SeccionCentral")
    if seccion is None:
        if fail_silently:
            print("[embalses] No se encontró SeccionCentral (posible bloqueo/HTML distinto).")
            return None
        raise RuntimeError("No se encontró SeccionCentral (posible bloqueo/HTML distinto).")

    lista = ["AGUA_TOTAL", "VARIACION", "CAPACIDAD", "MISMA SEMANA(t-1)", "MISMA SEMANA (t-10)"]
    filas = seccion.find_all("div", class_="FilaSeccion")
    if not filas:
        if fail_silently:
            print("[embalses] No se encontraron FilaSeccion.")
            return None
        raise RuntimeError("No se encontraron FilaSeccion.")

    dict_agua = {}

    for k, row in enumerate(filas[: len(lista)]):
        key = lista[k]
        dict_agua[key] = {"campo": None, "resultado1": None, "resultado2": None}

        try:
            campo_div = row.find("div", class_="Campo")
            campo_txt = campo_div.get_text(strip=True) if campo_div else ""

            if key == "AGUA_TOTAL":
                # Extrae dd-mm-YYYY dentro de paréntesis si existe
                if "(" in campo_txt and ")" in campo_txt:
                    campo_raw = campo_txt.split("(")[1].split(")")[0].strip()
                else:
                    campo_raw = campo_txt.strip()

                campo_dt = pd.to_datetime(campo_raw, format="%d-%m-%Y", errors="coerce")
                dict_agua[key]["campo"] = campo_dt.date().isoformat() if pd.notna(campo_dt) else None
            else:
                dict_agua[key]["campo"] = campo_txt or None

            resultados = row.find_all("div", class_="Resultado")
            for l, res in enumerate(resultados):
                val = _to_float_maybe(res.get_text(strip=True))
                if val is None:
                    continue

                if l == 0:
                    # OJO: VARIACION aquí viene en hm3 (miles con punto), queremos m3 como el resto
                    dict_agua[key]["resultado1"] = val 
                elif l == 1:
                    dict_agua[key]["resultado2"] = val

        except Exception as e:
            if fail_silently:
                print(f"[embalses] ERROR parse en {key}: {e!r}")
                continue
            raise

    return dict_agua


if __name__ == "__main__":
    data = dict_agua_embalses(fail_silently=False)

    out_dir = Path("data")
    out_dir.mkdir(exist_ok=True)

    out_path = out_dir / "embalses.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"[OK] Guardado {out_path.resolve()}")
