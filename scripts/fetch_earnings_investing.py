import os
import json
import pandas as pd
import cloudscraper
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

# --- Helpers env ---
def _env_str(name: str, default: str) -> str:
    return os.environ.get(name, default).strip()

def _env_list_int(name: str, default_csv: str) -> list[int]:
    raw = os.environ.get(name, default_csv).strip()
    if not raw:
        return []
    out = []
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            out.append(int(part))
    return out

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
        # Buscar día (número)
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

def scrape_earnings(date_from: str, date_to: str) -> pd.DataFrame:
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

    scraper = cloudscraper.create_scraper()
    scraper.trust_env = False
    scraper.proxies = {}

    # GET inicial para cookies
    scraper.get(base_url, headers=headers, timeout=40)

    # POST datos
    r = scraper.post(url, headers=headers, data=payload, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")

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

        if not empresa:
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
    temp["País2"] = temp["Country"].apply(lambda x: "ES" if x == "España" else ("EU" if x == "Eurozona" else "US"))
    temp["Evento2"] = temp["País2"] + " Resultado " + temp["Empresa"]

    return temp[["Día", "Evento2"]]

def next_monday_and_friday(base: date) -> tuple[date, date]:
    """
    Devuelve (lunes_siguiente, viernes_siguiente) relativo a base.
    - Si base es domingo -> lunes es mañana.
    - Si base es lunes -> lunes es hoy (puedes cambiarlo si quieres que sea el de la semana siguiente).
    """
    days_to_monday = (0 - base.weekday()) % 7  # Monday=0 ... Sunday=6
    start = base + timedelta(days=days_to_monday)
    end = start + timedelta(days=4)  # viernes
    return start, end

def main():
    # "hoy" en horario Madrid (runner suele ir en UTC)
    today_madrid = datetime.now(ZoneInfo("Europe/Madrid")).date()

    # Ventana: lunes->viernes (se puede sobreescribir por env si quieres)
    default_from, default_to = next_monday_and_friday(today_madrid)

    date_from = _env_str("DATE_FROM", default_from.isoformat())
    date_to = _env_str("DATE_TO", default_to.isoformat())

    df = scrape_earnings(date_from, date_to)

    os.makedirs("docs", exist_ok=True)
    df.to_csv("docs/calendar.csv", index=False)

    with open("docs/calendar.json", "w", encoding="utf-8") as f:
        json.dump(df.to_dict(orient="records"), f, ensure_ascii=False, indent=2)

    print(f"OK -> {len(df)} filas. docs/calendar.json y docs/calendar.csv generados.")

if __name__ == "__main__":
    main()

