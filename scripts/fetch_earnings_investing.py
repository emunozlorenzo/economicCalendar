import os
import json
import re
import pandas as pd
import cloudscraper
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from pathlib import Path

# --- Helpers env ---
def _env_str(name: str, default: str) -> str:
    return os.environ.get(name, default).strip()

def _env_list_int(name: str, default_csv: str) -> list[int]:
    raw = os.environ.get(name, default_csv).strip()
    if not raw:
        return []
    out: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            out.append(int(part))
    return out

# --- Stopwords / limpieza ---
stopwords_es = {
    "de", "del", "la", "las", "el", "los", "y", "a", "en", "que",
    "un", "una", "por", "con", "para", "se", "al", "lo", "su", "tras",
    "pero", "son", "etc"
}

def remove_stopwords_from_event(text: str, stopwords=stopwords_es) -> str:
    """Elimina stopwords sencillas de una cadena de texto."""
    tokens = text.split()
    tokens_filtrados = [t for t in tokens if t.lower() not in stopwords]
    return " ".join(tokens_filtrados)

# --- Fecha española (simple) ---
MONTHS_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12
}

def parse_spanish_date(s: str) -> str:
    """
    Convierte textos tipo: "vie. 13 feb." o "13 febrero" a "YYYY-MM-DD" (año actual si no aparece).
    Si falla, devuelve el string original.
    """
    try:
        txt = s.lower().replace(",", " ").replace(".", " ").strip()
        parts = [p for p in txt.split() if p]

        day = None
        month = None

        for p in parts:
            if p.isdigit():
                day = int(p)
                break

        for p in parts:
            if p in MONTHS_ES:
                month = MONTHS_ES[p]
                break

        if day and month:
            y = date.today().year
            return date(y, month, day).isoformat()
    except Exception:
        pass

    return s

# --- Aglutinado económico ---
def extraer_base_y_parens(evento: str) -> tuple[str, list[str]]:
    """
    Separa el texto principal de los paréntesis.
    Retorna (base, [paréntesis]).
    """
    parens = re.findall(r"\((.*?)\)", evento)
    base = re.sub(r"\(.*?\)", "", evento).strip()
    return base, parens

def aglutinar_eventos_por_dia(df: pd.DataFrame) -> pd.DataFrame:
    """
    Espera DF con columnas ['Día','Evento2'] y devuelve DF aglutinado:
    - Agrupa por (Día, base) fusionando paréntesis
    - Normaliza paréntesis (Anual->YoY, etc.)
    - Limpia con remove_stopwords_from_event
    """
    if df.empty:
        return pd.DataFrame(columns=["Día", "Evento2"])

    agrupado: dict[tuple[str, str], set[str]] = {}

    for _, row in df.iterrows():
        dia = str(row.get("Día", "")).strip()
        evento = str(row.get("Evento2", "")).strip()
        if not dia or not evento:
            continue

        base, parens = extraer_base_y_parens(evento)
        clave = (dia, base)
        if clave not in agrupado:
            agrupado[clave] = set()

        # Puede venir un paréntesis con varios tokens: "(Anual/1T)" etc.
        for p in parens:
            for token in re.split(r"[/,;]\s*", p.strip()):
                t = token.strip()
                if t:
                    agrupado[clave].add(t)

    reemplazos_parens = {
        "Anual": "YoY",
        "Trimestral": "QoQ",
        "Mensual": "MoM",
        "1T": "Q1",
        "2T": "Q2",
        "3T": "Q3",
        "4T": "Q4",
    }

    filas_nuevas = []
    for (dia, base), conjunto_parens in agrupado.items():
        lista_parens = sorted(conjunto_parens)
        lista_parens = [reemplazos_parens.get(elem, elem) for elem in lista_parens]

        if lista_parens:
            evento_final = f"{base} ({'/'.join(lista_parens)})"
        else:
            evento_final = base

        evento_final_limpio = remove_stopwords_from_event(evento_final)
        filas_nuevas.append({"Día": dia, "Evento2": evento_final_limpio})

    out = pd.DataFrame(filas_nuevas)
    if out.empty:
        return pd.DataFrame(columns=["Día", "Evento2"])

    return out.sort_values(["Día", "Evento2"]).reset_index(drop=True)

# --- Scrapers ---
def scrape_earnings(scraper, date_from: str, date_to: str) -> pd.DataFrame:
    """
    Devuelve DataFrame con columnas: Día (YYYY-MM-DD) y Evento2 (p.ej. "ES Resultado EmpresaX")
    Configurable por env:
      INVESTING_LANG_HOST=es.investing.com
      INVESTING_COUNTRIES=26,5,...
      INVESTING_IMPORTANCE=1,2,3
      INVESTING_TIMEZONE=80
    """
    host = _env_str("INVESTING_LANG_HOST", "es.investing.com")
    countries = _env_list_int("INVESTING_COUNTRIES", "26")       # España por defecto
    importance = _env_list_int("INVESTING_IMPORTANCE", "3")      # alta por defecto
    tz = _env_str("INVESTING_TIMEZONE", "80")

    base_url = f"https://{host}/earnings-calendar/"
    url = f"https://{host}/earnings-calendar/Service/getCalendarFilteredData"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        ),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": base_url,
        "Origin": f"https://{host}",
    }

    payload = {
        "dateFrom": date_from,
        "dateTo": date_to,
        "country[]": countries,
        "importance[]": importance,
        "timeZone": tz,
        "action": "getCalendarFilteredData",
    }

    # GET inicial para cookies
    scraper.get(base_url, headers=headers, timeout=40)

    # POST datos
    r = scraper.post(url, headers=headers, data=payload, timeout=60)
    if r.status_code != 200:
        return pd.DataFrame(columns=["Día", "Evento2"])

    data = r.json()
    if "data" not in data or not data["data"]:
        return pd.DataFrame(columns=["Día", "Evento2"])

    soup = BeautifulSoup(data["data"], "lxml")
    rows = soup.find_all("tr")

    resultados = []
    current_day_parsed = None

    for row in rows:
        day_cell = row.find("td", class_="theDay")
        if day_cell:
            current_day_text = day_cell.get_text(strip=True)
            current_day_parsed = parse_spanish_date(current_day_text)
            continue

        tds = row.find_all("td")
        if not tds:
            continue

        company_cell = row.find("td", class_="left noWrap earnCalCompany")
        if not company_cell:
            continue

        flag_span = tds[0].find("span", class_="ceFlags")
        country = flag_span.get("title", "").strip() if flag_span else "Desconocido"

        empresa_span = tds[1].find("span", class_="earnCalCompanyName")
        empresa = empresa_span.get_text(strip=True) if empresa_span else ""
        empresa = empresa.strip()

        if not empresa or not current_day_parsed:
            continue

        resultados.append({
            "Día": current_day_parsed,
            "Country": country,
            "Empresa": empresa,
        })

    df = pd.DataFrame(resultados)
    if df.empty:
        return pd.DataFrame(columns=["Día", "Evento2"])

    temp = df.copy()
    temp["País2"] = temp["Country"].apply(
        lambda x: "ES" if x == "España" else ("EU" if x == "Eurozona" else "US")
    )
    temp["Evento2"] = temp["País2"] + " Resultado " + temp["Empresa"]

    return temp[["Día", "Evento2"]].drop_duplicates().reset_index(drop=True)

def scrape_economic(scraper, date_from: str, date_to: str) -> pd.DataFrame:
    """
    Scrapear el calendario económico y retornar un DataFrame con columnas:
    'Día' y 'Evento2' (ej.: "EU PIB ... (YoY/Q1)").

    Configurable por env:
      INVESTING_LANG_HOST=es.investing.com
      ECON_COUNTRIES=5,26,72
      ECON_IMPORTANCE=2,3
      INVESTING_TIMEZONE=80
    """
    host = _env_str("INVESTING_LANG_HOST", "es.investing.com")
    tz = _env_str("INVESTING_TIMEZONE", "80")
    econ_countries = _env_list_int("ECON_COUNTRIES", "5,26,72")
    econ_importance = _env_list_int("ECON_IMPORTANCE", "3")

    base_url = f"https://{host}/economic-calendar/"
    url = f"https://{host}/economic-calendar/Service/getCalendarFilteredData"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        ),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": base_url,
        "Origin": f"https://{host}",
    }

    payload = {
        "tab": "all",
        "dateFrom": date_from,
        "dateTo": date_to,
        "timeZone": tz,
        "timeFilter": "timeOnly",
        "country[]": econ_countries,
        "importance[]": econ_importance,
        "action": "getCalendarFilteredData",
    }

    # GET inicial para cookies
    scraper.get(base_url, headers=headers, timeout=40)

    r = scraper.post(url, headers=headers, data=payload, timeout=60)
    if r.status_code != 200:
        return pd.DataFrame(columns=["Día", "Evento2"])

    data = r.json()
    if "data" not in data or not data["data"]:
        return pd.DataFrame(columns=["Día", "Evento2"])

    soup = BeautifulSoup(data["data"], "lxml")
    rows = soup.find_all("tr", class_="js-event-item")

    eventos = []
    for row in rows:
        datetime_str = row.attrs.get("data-event-datetime", "")
        dia = datetime_str.split(" ")[0] if datetime_str else ""

        pais_el = row.find("span", class_="ceFlags")
        pais = pais_el.get("title", "").strip() if pais_el else ""

        a_tag = row.find("a")
        evento = a_tag.get_text(strip=True) if a_tag else ""

        if not dia or not evento:
            continue

        eventos.append({
            "Día": dia,
            "País": pais,
            "Evento": evento,
        })

    df = pd.DataFrame(eventos)
    if df.empty:
        return pd.DataFrame(columns=["Día", "Evento2"])

    temp = df.copy()
    temp["País2"] = temp["País"].apply(
        lambda x: "ES" if x == "España" else ("EU" if x == "Eurozona" else "US")
    )

    # Mantengo el nombre "Evento2" para compatibilidad con vuestra lógica
    temp["Evento2"] = temp["País2"] + " " + temp["Evento"]

    out = temp[["Día", "Evento2"]].drop_duplicates().reset_index(drop=True)

    # Aglutinado + limpieza final
    out = aglutinar_eventos_por_dia(out)
    return out

# --- Ventana lunes->viernes ---
from datetime import date, timedelta

def monday_to_friday(base: date, when: str = "current"):
    # base.weekday(): lunes=0 ... domingo=6

    # 1) calculo el lunes de la semana en la que está "base"
    start = base - timedelta(days=base.weekday())

    # 2) si quieren la semana siguiente, sumo 7 días
    if when == "next":
        start = start + timedelta(days=7)
    elif when == "current":
        pass  # me quedo con la semana actual
    else:
        raise ValueError("when debe ser 'current' o 'next'")

    # 3) viernes = lunes + 4 días
    end = start + timedelta(days=4)

    return start, end


def main():
    # "hoy" en horario Madrid (runner suele ir en UTC)
    today_madrid = datetime.now(ZoneInfo("Europe/Madrid")).date()

    # Ventana: lunes->viernes (se puede sobreescribir por env si quieres)
    default_from, default_to = monday_to_friday(today_madrid,"current")
    date_from = _env_str("DATE_FROM", default_from.isoformat())
    date_to = _env_str("DATE_TO", default_to.isoformat())

    # Un único scraper compartido
    scraper = cloudscraper.create_scraper()
    scraper.trust_env = False
    scraper.proxies = {}

    df_econ = scrape_economic(scraper, date_from, date_to)
    df_earn = scrape_earnings(scraper, date_from, date_to)

    df_all = pd.concat([df_econ, df_earn], ignore_index=True)
    df_all = df_all.dropna(subset=["Día", "Evento2"])
    df_all = df_all.drop_duplicates().sort_values(["Día", "Evento2"]).reset_index(drop=True)

    # Escribe SIEMPRE relativo a la raíz del repo (no al CWD)
    repo_root = Path(__file__).resolve().parents[1]  # .../scripts/ -> repo root
    out_dir = repo_root / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / "calendar.csv"
    json_path = out_dir / "calendar.json"

    df_all.to_csv(csv_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(df_all.to_dict(orient="records"), f, ensure_ascii=False, indent=2)

    print(
        f"OK -> {len(df_all)} filas (econ={len(df_econ)}, earn={len(df_earn)}). "
        f"{json_path} y {csv_path} generados."
    )

if __name__ == "__main__":
    main()
