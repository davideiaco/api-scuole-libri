from __future__ import annotations

import json
import html
import logging
import os
import re
import sys
import time
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


# =========================================================
# PATH / ENV
# =========================================================
def get_app_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


APP_DIR = get_app_dir()
ENV_PATH = APP_DIR / "config.env"
load_dotenv(dotenv_path=ENV_PATH)


# =========================================================
# APP
# =========================================================
app = FastAPI(title="API Scuole + Libri + Shopify")


# =========================================================
# LOGGING
# =========================================================
logger = logging.getLogger("api_scuole_libri")
logger.setLevel(logging.INFO)
logger.propagate = False

if not logger.handlers:
    _console = logging.StreamHandler()
    _console.setLevel(logging.INFO)
    _console.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(_console)


def log_error(msg: str, details: Any = None, *, max_chars: int = 2500) -> None:
    if details is None:
        logger.error(msg)
        return

    try:
        s = json.dumps(details, ensure_ascii=False, indent=2, default=str)
    except Exception:
        s = str(details)

    if len(s) > max_chars:
        s = s[:max_chars] + f"\n…(troncato, {len(s)} chars totali)"

    logger.error(f"{msg}\n{s}")


# =========================================================
# CORS
# =========================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # in produzione restringi
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================================================
# CONFIG
# =========================================================
MIUR_SCUOLE_ENDPOINT = "https://dati.istruzione.it/opendata/SCUANAGRAFESTAT/query"
MIUR_SCUOLE_AUTONOME_ENDPOINT = "https://dati.istruzione.it/opendata/SCUANAAUTSTAT/query"
MIUR_OPENDATA_BASE = "https://dati.istruzione.it/opendata"

HTTP_TIMEOUT = 60
SPARQL_PAGE_SIZE = 1000
USER_AGENT = "fastapi-scuole-libri/6.0"

SHOPIFY_SHOP = os.getenv("SHOPIFY_SHOP", "").strip()
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN", "").strip()
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2026-01").strip()
SHOPIFY_LOCATION_ID = os.getenv("SHOPIFY_LOCATION_ID", "").strip()

SHOPIFY_REFRESH_CLIENT_ID = os.getenv("SHOPIFY_REFRESH_CLIENT_ID", "").strip()
SHOPIFY_REFRESH_CLIENT_SECRET = os.getenv("SHOPIFY_REFRESH_CLIENT_SECRET", "").strip()

EXTERNAL_ID_NAMESPACE = "custom"
EXTERNAL_ID_KEY = "external_id"


def env_csv(name: str, default: str = "") -> List[str]:
    raw = os.getenv(name, default).strip()
    return [x.strip() for x in raw.split(",") if x.strip()]


PUBLICATION_IDS = env_csv("SHOPIFY_PUBLICATION_IDS", "")


# =========================================================
# CACHE TTL SEMPLICE
# =========================================================
class TTLCache:
    def __init__(self, ttl_seconds: int = 3600, max_items: int = 1024):
        self.ttl_seconds = ttl_seconds
        self.max_items = max_items
        self._store: Dict[str, Tuple[float, Any]] = {}
        self._lock = RLock()

    def get(self, key: str) -> Optional[Any]:
        now = time.time()
        with self._lock:
            item = self._store.get(key)
            if not item:
                return None

            expires_at, value = item
            if expires_at < now:
                self._store.pop(key, None)
                return None

            return value

    def set(self, key: str, value: Any) -> None:
        now = time.time()
        with self._lock:
            if len(self._store) >= self.max_items:
                self._evict(now)
            self._store[key] = (now + self.ttl_seconds, value)

    def _evict(self, now: float) -> None:
        expired_keys = [k for k, (exp, _) in self._store.items() if exp < now]
        for key in expired_keys[: max(1, self.max_items // 10)]:
            self._store.pop(key, None)

        if len(self._store) >= self.max_items:
            first_key = next(iter(self._store), None)
            if first_key is not None:
                self._store.pop(first_key, None)


cache_province = TTLCache(ttl_seconds=24 * 3600, max_items=128)
cache_comuni = TTLCache(ttl_seconds=24 * 3600, max_items=1024)
cache_scuole = TTLCache(ttl_seconds=24 * 3600, max_items=8192)
cache_search = TTLCache(ttl_seconds=6 * 3600, max_items=4096)
cache_libri = TTLCache(ttl_seconds=24 * 3600, max_items=8192)
cache_shopify_lookup = TTLCache(ttl_seconds=6 * 3600, max_items=4096)


# =========================================================
# COSTANTI DOMINIO
# =========================================================
REGIONI_CANONICHE = [
    "ABRUZZO",
    "BASILICATA",
    "CALABRIA",
    "CAMPANIA",
    "EMILIA-ROMAGNA",
    "FRIULI-VENEZIA GIULIA",
    "LAZIO",
    "LIGURIA",
    "LOMBARDIA",
    "MARCHE",
    "MOLISE",
    "PIEMONTE",
    "PUGLIA",
    "SARDEGNA",
    "SICILIA",
    "TOSCANA",
    "TRENTINO-ALTO ADIGE",
    "UMBRIA",
    "VALLE D'AOSTA",
    "VENETO",
]

REGION_ALIASES = {
    "ABRUZZO": "ABRUZZO",
    "BASILICATA": "BASILICATA",
    "CALABRIA": "CALABRIA",
    "CAMPANIA": "CAMPANIA",
    "EMILIA ROMAGNA": "EMILIA-ROMAGNA",
    "EMILIA-ROMAGNA": "EMILIA-ROMAGNA",
    "FRIULI VENEZIA GIULIA": "FRIULI-VENEZIA GIULIA",
    "FRIULI-VENEZIA GIULIA": "FRIULI-VENEZIA GIULIA",
    "LAZIO": "LAZIO",
    "LIGURIA": "LIGURIA",
    "LOMBARDIA": "LOMBARDIA",
    "MARCHE": "MARCHE",
    "MOLISE": "MOLISE",
    "PIEMONTE": "PIEMONTE",
    "PUGLIA": "PUGLIA",
    "SARDEGNA": "SARDEGNA",
    "SICILIA": "SICILIA",
    "TOSCANA": "TOSCANA",
    "TRENTINO ALTO ADIGE": "TRENTINO-ALTO ADIGE",
    "TRENTINO-ALTO ADIGE": "TRENTINO-ALTO ADIGE",
    "TRENTINO-ALTO ADIGE/SUDTIROL": "TRENTINO-ALTO ADIGE",
    "TRENTINO-ALTO ADIGE/SÜDTIROL": "TRENTINO-ALTO ADIGE",
    "UMBRIA": "UMBRIA",
    "VALLE D AOSTA": "VALLE D'AOSTA",
    "VALLE D'AOSTA": "VALLE D'AOSTA",
    "VALLEE D AOSTE": "VALLE D'AOSTA",
    "VALLE D'AOSTA/VALLEE D'AOSTE": "VALLE D'AOSTA",
    "VENETO": "VENETO",
}

ALT_DATASET_BY_REGION = {
    "ABRUZZO": "ALTABRUZZO",
    "BASILICATA": "ALTBASILICATA",
    "CALABRIA": "ALTCALABRIA",
    "CAMPANIA": "ALTCAMPANIA",
    "EMILIA-ROMAGNA": "ALTEMILIAROMAGNA",
    "FRIULI-VENEZIA GIULIA": "ALTFRIULIVENEZIAGIULIA",
    "LAZIO": "ALTLAZIO",
    "LIGURIA": "ALTLIGURIA",
    "LOMBARDIA": "ALTLOMBARDIA",
    "MARCHE": "ALTMARCHE",
    "MOLISE": "ALTMOLISE",
    "PIEMONTE": "ALTPIEMONTE",
    "PUGLIA": "ALTPUGLIA",
    "SARDEGNA": "ALTSARDEGNA",
    "SICILIA": "ALTSICILIA",
    "TOSCANA": "ALTTOSCANA",
    "TRENTINO-ALTO ADIGE": "ALTTRENTINOALTOADIGE",
    "UMBRIA": "ALTUMBRIA",
    "VALLE D'AOSTA": "ALTVALLEDAOSTA",
    "VENETO": "ALTVENETO",
}


# =========================================================
# HTTP SESSION
# =========================================================
http_session = requests.Session()
http_session.headers.update(
    {
        "Accept": "application/sparql-results+json, application/json;q=0.9, */*;q=0.8",
        "User-Agent": USER_AGENT,
    }
)


# =========================================================
# UTILS
# =========================================================
def norm(value: Optional[str]) -> str:
    return (value or "").strip().upper()


def normalize_spaces(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def normalize_regione_input(value: str) -> str:
    cleaned = normalize_spaces(value).upper().replace("’", "'")
    if cleaned in REGION_ALIASES:
        return REGION_ALIASES[cleaned]

    fallback = re.sub(r"[^A-Z0-9/' -]", " ", cleaned)
    fallback = re.sub(r"\s+", " ", fallback).strip()
    if fallback in REGION_ALIASES:
        return REGION_ALIASES[fallback]

    raise HTTPException(status_code=400, detail=f"Regione non riconosciuta: {value}")


def scuole_endpoint_for_regione(regione: str) -> str:
    if regione in {"TRENTINO-ALTO ADIGE", "VALLE D'AOSTA"}:
        return MIUR_SCUOLE_AUTONOME_ENDPOINT
    return MIUR_SCUOLE_ENDPOINT


def regione_for_scuole_endpoint(regione: str) -> str:
    if regione == "EMILIA-ROMAGNA":
        return "EMILIA ROMAGNA"
    if regione == "FRIULI-VENEZIA GIULIA":
        return "FRIULI-VENEZIA G."
    if regione == "TRENTINO-ALTO ADIGE":
        return "TRENTINO-ALTO ADIGE"
    if regione == "VALLE D'AOSTA":
        return "VALLE D' AOSTA"
    return regione


def require_not_blank(value: str, field_name: str) -> str:
    cleaned = normalize_spaces(value)
    if not cleaned:
        raise HTTPException(status_code=400, detail=f"Il parametro '{field_name}' è obbligatorio")
    return cleaned


def sparql_escape_string(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", " ")
        .replace("\r", " ")
    )


def binding_value(item: Dict[str, Any], key: str) -> Optional[str]:
    return item.get(key, {}).get("value")


def build_cache_key(prefix: str, *parts: Any) -> str:
    return "::".join([prefix, *[str(part) for part in parts]])


def extract_bindings(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    return payload.get("results", {}).get("bindings", [])


def extract_shopify_numeric_id(gid: Optional[str]) -> Optional[str]:
    if not gid:
        return None
    return gid.rsplit("/", 1)[-1].strip() or None


def session_get_json(url: str, *, params: Dict[str, str]) -> Dict[str, Any]:
    try:
        response = http_session.get(
            url,
            params=params,
            timeout=HTTP_TIMEOUT,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Errore chiamando il servizio MIUR: {exc}",
        ) from exc

    try:
        return response.json()
    except ValueError as exc:
        raise HTTPException(
            status_code=502,
            detail="Risposta non valida dal servizio MIUR",
        ) from exc


def execute_sparql(endpoint: str, query: str) -> List[Dict[str, Any]]:
    payload = session_get_json(endpoint, params={"query": query})
    return extract_bindings(payload)


# =========================================================
# SHOPIFY TOKEN REFRESH
# =========================================================
def _shop_name_only(shop_value: str) -> str:
    shop_value = (shop_value or "").strip()
    if not shop_value:
        return ""
    return (
        shop_value
        .replace("https://", "")
        .replace("http://", "")
        .replace(".myshopify.com", "")
        .strip("/")
    )


def update_config_env_access_token(new_token: str, file_path: Path = ENV_PATH) -> None:
    if not new_token:
        raise ValueError("Nuovo access token vuoto")

    if file_path.exists():
        content = file_path.read_text(encoding="utf-8")
    else:
        content = ""

    pattern = r"(?m)^SHOPIFY_ACCESS_TOKEN=.*$"
    replacement = f"SHOPIFY_ACCESS_TOKEN={new_token}"

    if re.search(pattern, content):
        new_content = re.sub(pattern, replacement, content)
    else:
        if content and not content.endswith("\n"):
            content += "\n"
        new_content = content + replacement + "\n"

    file_path.write_text(new_content, encoding="utf-8")


def refresh_shopify_access_token() -> str:
    global SHOPIFY_ACCESS_TOKEN

    shop_name = _shop_name_only(SHOPIFY_SHOP)
    if not shop_name:
        raise RuntimeError("SHOPIFY_SHOP mancante o non valido")

    refresh_url = f"https://{shop_name}.myshopify.com/admin/oauth/access_token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": SHOPIFY_REFRESH_CLIENT_ID,
        "client_secret": SHOPIFY_REFRESH_CLIENT_SECRET,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    logger.info("Token Shopify non valido/scaduto: rigenerazione access token in corso")

    resp = requests.post(refresh_url, headers=headers, data=payload, timeout=60)
    resp.raise_for_status()

    data = resp.json()
    new_token = (data.get("access_token") or "").strip()
    if not new_token:
        log_error("Rigenerazione token fallita: risposta senza access_token", data)
        raise RuntimeError("Risposta refresh token senza access_token")

    update_config_env_access_token(new_token)
    SHOPIFY_ACCESS_TOKEN = new_token
    os.environ["SHOPIFY_ACCESS_TOKEN"] = new_token

    logger.info("Nuovo access token Shopify generato e salvato in: %s", str(ENV_PATH.resolve()))
    return new_token


def _graphql_has_auth_error(data: Dict[str, Any]) -> bool:
    if not isinstance(data, dict):
        return False

    errors = data.get("errors") or []
    for err in errors:
        msg = str((err or {}).get("message") or "").lower()
        ext_code = str(((err or {}).get("extensions") or {}).get("code") or "").lower()

        if (
            "invalid api key or access token" in msg
            or "access denied" in msg
            or "unauthorized" in msg
            or "forbidden" in msg
            or ext_code in {"unauthorized", "forbidden", "access_denied"}
        ):
            return True

    return False


def _is_auth_http_error(resp: Optional[requests.Response]) -> bool:
    if resp is None:
        return False
    return resp.status_code in (401, 403)


# =========================================================
# SHOPIFY GRAPHQL
# =========================================================
def shopify_endpoint() -> str:
    if not SHOPIFY_SHOP or not SHOPIFY_ACCESS_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="Configurazione Shopify mancante: SHOPIFY_SHOP / SHOPIFY_ACCESS_TOKEN",
        )
    return f"https://{SHOPIFY_SHOP}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"


def shopify_graphql(
    query: str,
    variables: Dict[str, Any],
    *,
    max_retries: int = 6,
) -> Dict[str, Any]:
    global SHOPIFY_ACCESS_TOKEN

    endpoint = shopify_endpoint()
    current_token = SHOPIFY_ACCESS_TOKEN
    auth_refresh_done = False

    for attempt in range(max_retries):
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": current_token,
        }

        try:
            resp = requests.post(
                endpoint,
                headers=headers,
                json={"query": query, "variables": variables},
                timeout=180,
            )

            if _is_auth_http_error(resp):
                if auth_refresh_done:
                    resp.raise_for_status()

                current_token = refresh_shopify_access_token()
                SHOPIFY_ACCESS_TOKEN = current_token
                auth_refresh_done = True
                logger.info("Retry chiamata Shopify GraphQL con nuovo access token")
                continue

            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)

            resp.raise_for_status()
            data = resp.json()

            if _graphql_has_auth_error(data):
                if auth_refresh_done:
                    log_error("Errore autenticazione Shopify anche dopo refresh token", data)
                    raise RuntimeError("Autenticazione Shopify fallita anche dopo refresh token")

                current_token = refresh_shopify_access_token()
                SHOPIFY_ACCESS_TOKEN = current_token
                auth_refresh_done = True
                logger.info("Retry GraphQL dopo refresh token per errore auth applicativo")
                continue

            if "errors" in data and data["errors"]:
                log_error("GraphQL top-level errors", data["errors"])
                raise HTTPException(status_code=502, detail="GraphQL top-level errors da Shopify")

            return data

        except HTTPException:
            raise
        except Exception as e:
            if attempt >= max_retries - 1:
                log_error(
                    "Errore chiamata Shopify GraphQL (ultimo tentativo)",
                    {
                        "error": str(e),
                        "status": getattr(getattr(e, "response", None), "status_code", None),
                    },
                )
                raise HTTPException(
                    status_code=502,
                    detail=f"Errore chiamata Shopify GraphQL: {e}",
                ) from e

            sleep_s = min(2 ** attempt, 30)
            time.sleep(sleep_s)

    raise HTTPException(status_code=502, detail="Errore Shopify non recuperabile")


QUERY_PRODUCT_BY_CUSTOM_ID = """
query ProductByCustomId($identifier: ProductIdentifierInput!) {
  productByIdentifier(identifier: $identifier) {
    id
    title
    status
    variants(first: 5) {
      nodes {
        id
        sku
        barcode
        inventoryPolicy
        inventoryItem {
          id
          tracked
        }
      }
    }
  }
}
""".strip()

MUTATION_PRODUCT_SET_MINIMAL = """
mutation ProductSetMinimal(
  $identifier: ProductSetIdentifiers
  $input: ProductSetInput!
  $synchronous: Boolean!
) {
  productSet(identifier: $identifier, input: $input, synchronous: $synchronous) {
    product {
      id
      title
      status
      variants(first: 5) {
        nodes {
          id
          sku
          barcode
          inventoryPolicy
          inventoryItem {
            id
            tracked
          }
        }
      }
    }
    userErrors {
      field
      message
    }
  }
}
""".strip()

MUTATION_METAFIELDS_SET = """
mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields {
      id
      namespace
      key
      value
    }
    userErrors {
      field
      message
      code
    }
  }
}
""".strip()

MUTATION_PUBLISHABLE_PUBLISH = """
mutation PublishProduct($id: ID!, $input: [PublicationInput!]!) {
  publishablePublish(id: $id, input: $input) {
    publishable {
      ... on Product {
        id
      }
    }
    userErrors {
      field
      message
    }
  }
}
""".strip()


# =========================================================
# QUERY BUILDERS MIUR
# =========================================================
def build_province_query(regione: str) -> str:
    regione_safe = sparql_escape_string(regione_for_scuole_endpoint(regione).lower())
    return f"""
PREFIX miur: <http://www.miur.it/ns/miur#>

SELECT ?Provincia (COUNT(DISTINCT ?S) AS ?Totale)
WHERE {{
  GRAPH ?g {{
    ?S miur:REGIONE ?Regione .
    ?S miur:PROVINCIA ?Provincia .
    FILTER (lcase(str(?Regione)) = "{regione_safe}")
  }}
}}
GROUP BY ?Provincia
ORDER BY ?Provincia
""".strip()


def build_comuni_query(regione: str, provincia: str) -> str:
    regione_safe = sparql_escape_string(regione_for_scuole_endpoint(regione).lower())
    provincia_safe = sparql_escape_string(provincia.lower())
    return f"""
PREFIX miur: <http://www.miur.it/ns/miur#>

SELECT ?DescrizioneComune (COUNT(DISTINCT ?S) AS ?Totale)
WHERE {{
  GRAPH ?g {{
    ?S miur:REGIONE ?Regione .
    ?S miur:PROVINCIA ?Provincia .
    ?S miur:DESCRIZIONECOMUNE ?DescrizioneComune .
    FILTER (lcase(str(?Regione)) = "{regione_safe}")
    FILTER (lcase(str(?Provincia)) = "{provincia_safe}")
  }}
}}
GROUP BY ?DescrizioneComune
ORDER BY ?DescrizioneComune
""".strip()


def build_scuole_count_query(regione: str, provincia: str, comune: str) -> str:
    regione_safe = sparql_escape_string(regione_for_scuole_endpoint(regione).lower())
    provincia_safe = sparql_escape_string(provincia.lower())
    comune_safe = sparql_escape_string(comune.lower())
    return f"""
PREFIX miur: <http://www.miur.it/ns/miur#>

SELECT (COUNT(DISTINCT ?CodiceScuola) AS ?Totale)
WHERE {{
  GRAPH ?g {{
    ?S miur:REGIONE ?Regione .
    ?S miur:PROVINCIA ?Provincia .
    ?S miur:DESCRIZIONECOMUNE ?DescrizioneComune .
    ?S miur:CODICESCUOLA ?CodiceScuola .
    FILTER (lcase(str(?Regione)) = "{regione_safe}")
    FILTER (lcase(str(?Provincia)) = "{provincia_safe}")
    FILTER (lcase(str(?DescrizioneComune)) = "{comune_safe}")
  }}
}}
""".strip()


def build_scuole_query(regione: str, provincia: str, comune: str, limit: int, offset: int) -> str:
    regione_safe = sparql_escape_string(regione_for_scuole_endpoint(regione).lower())
    provincia_safe = sparql_escape_string(provincia.lower())
    comune_safe = sparql_escape_string(comune.lower())

    return f"""
PREFIX miur: <http://www.miur.it/ns/miur#>

SELECT DISTINCT
  ?CodiceScuola
  ?DenominazioneScuola
  ?IndirizzoScuola
  ?IndirizzoEmailScuola
  ?IndirizzoPecScuola
  ?SitoWebScuola
  ?DescrizioneTipologiaGradoIstruzioneScuola
  ?DescrizioneCaratteristicaScuola
WHERE {{
  GRAPH ?g {{
    ?S miur:REGIONE ?Regione .
    ?S miur:PROVINCIA ?Provincia .
    ?S miur:DESCRIZIONECOMUNE ?DescrizioneComune .
    ?S miur:CODICESCUOLA ?CodiceScuola .
    ?S miur:DENOMINAZIONESCUOLA ?DenominazioneScuola .

    OPTIONAL {{ ?S miur:INDIRIZZOSCUOLA ?IndirizzoScuola . }}
    OPTIONAL {{ ?S miur:INDIRIZZOEMAILSCUOLA ?IndirizzoEmailScuola . }}
    OPTIONAL {{ ?S miur:INDIRIZZOPECSCUOLA ?IndirizzoPecScuola . }}
    OPTIONAL {{ ?S miur:SITOWEBSCUOLA ?SitoWebScuola . }}
    OPTIONAL {{ ?S miur:DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA ?DescrizioneTipologiaGradoIstruzioneScuola . }}
    OPTIONAL {{ ?S miur:DESCRIZIONECARATTERISTICASCUOLA ?DescrizioneCaratteristicaScuola . }}

    FILTER (lcase(str(?Regione)) = "{regione_safe}")
    FILTER (lcase(str(?Provincia)) = "{provincia_safe}")
    FILTER (lcase(str(?DescrizioneComune)) = "{comune_safe}")
  }}
}}
ORDER BY ?DenominazioneScuola ?CodiceScuola
LIMIT {limit}
OFFSET {offset}
""".strip()


def build_scuole_search_count_query(regione: str, q: str) -> str:
    regione_safe = sparql_escape_string(regione_for_scuole_endpoint(regione).lower())
    q_safe = sparql_escape_string(q.lower())
    return f"""
PREFIX miur: <http://www.miur.it/ns/miur#>

SELECT (COUNT(DISTINCT ?CodiceScuola) AS ?Totale)
WHERE {{
  GRAPH ?g {{
    ?S miur:REGIONE ?Regione .
    ?S miur:PROVINCIA ?Provincia .
    ?S miur:DESCRIZIONECOMUNE ?DescrizioneComune .
    ?S miur:CODICESCUOLA ?CodiceScuola .
    ?S miur:DENOMINAZIONESCUOLA ?DenominazioneScuola .

    FILTER (lcase(str(?Regione)) = "{regione_safe}")
    FILTER (
      CONTAINS(lcase(str(?DenominazioneScuola)), "{q_safe}") ||
      CONTAINS(lcase(str(?CodiceScuola)), "{q_safe}") ||
      CONTAINS(lcase(str(?DescrizioneComune)), "{q_safe}") ||
      CONTAINS(lcase(str(?Provincia)), "{q_safe}")
    )
  }}
}}
""".strip()


def build_scuole_search_query(regione: str, q: str, limit: int, offset: int) -> str:
    regione_safe = sparql_escape_string(regione_for_scuole_endpoint(regione).lower())
    q_safe = sparql_escape_string(q.lower())
    return f"""
PREFIX miur: <http://www.miur.it/ns/miur#>

SELECT DISTINCT
  ?Provincia
  ?DescrizioneComune
  ?CodiceScuola
  ?DenominazioneScuola
  ?IndirizzoScuola
  ?SitoWebScuola
WHERE {{
  GRAPH ?g {{
    ?S miur:REGIONE ?Regione .
    ?S miur:PROVINCIA ?Provincia .
    ?S miur:DESCRIZIONECOMUNE ?DescrizioneComune .
    ?S miur:CODICESCUOLA ?CodiceScuola .
    ?S miur:DENOMINAZIONESCUOLA ?DenominazioneScuola .

    OPTIONAL {{ ?S miur:INDIRIZZOSCUOLA ?IndirizzoScuola . }}
    OPTIONAL {{ ?S miur:SITOWEBSCUOLA ?SitoWebScuola . }}

    FILTER (lcase(str(?Regione)) = "{regione_safe}")
    FILTER (
      CONTAINS(lcase(str(?DenominazioneScuola)), "{q_safe}") ||
      CONTAINS(lcase(str(?CodiceScuola)), "{q_safe}") ||
      CONTAINS(lcase(str(?DescrizioneComune)), "{q_safe}") ||
      CONTAINS(lcase(str(?Provincia)), "{q_safe}")
    )
  }}
}}
ORDER BY ?DenominazioneScuola ?CodiceScuola
LIMIT {limit}
OFFSET {offset}
""".strip()


def build_libri_query(codicescuola: str, limit: int, offset: int) -> str:
    codice_safe = sparql_escape_string(codicescuola.strip().lower())
    return f"""
PREFIX miur: <http://www.miur.it/ns/miur#>

SELECT
  ?CodiceScuola ?AnnoCorso ?SezioneAnno ?TipoGradoScuola ?Combinazione
  ?Disciplina ?CodiceISBN ?Autori ?Titolo ?Sottotitolo ?Volume
  ?Editore ?Prezzo ?NuovaAdoz ?DaAcquist ?Consigliato
WHERE {{
  GRAPH ?g {{
    ?S miur:CODICESCUOLA ?CodiceScuola .
    ?S miur:ANNOCORSO ?AnnoCorso .
    ?S miur:SEZIONEANNO ?SezioneAnno .

    OPTIONAL {{ ?S miur:TIPOGRADOSCUOLA ?TipoGradoScuola . }}
    OPTIONAL {{ ?S miur:COMBINAZIONE ?Combinazione . }}
    OPTIONAL {{ ?S miur:DISCIPLINA ?Disciplina . }}
    OPTIONAL {{ ?S miur:CODICEISBN ?CodiceISBN . }}
    OPTIONAL {{ ?S miur:AUTORI ?Autori . }}
    OPTIONAL {{ ?S miur:TITOLO ?Titolo . }}
    OPTIONAL {{ ?S miur:SOTTOTITOLO ?Sottotitolo . }}
    OPTIONAL {{ ?S miur:VOLUME ?Volume . }}
    OPTIONAL {{ ?S miur:EDITORE ?Editore . }}
    OPTIONAL {{ ?S miur:PREZZO ?Prezzo . }}
    OPTIONAL {{ ?S miur:NUOVAADOZ ?NuovaAdoz . }}
    OPTIONAL {{ ?S miur:DAACQUIST ?DaAcquist . }}
    OPTIONAL {{ ?S miur:CONSIGLIATO ?Consigliato . }}

    FILTER (lcase(str(?CodiceScuola)) = "{codice_safe}")
  }}
}}
ORDER BY ?SezioneAnno ?AnnoCorso ?Disciplina ?Titolo ?CodiceISBN
LIMIT {limit}
OFFSET {offset}
""".strip()


# =========================================================
# PARSERS MIUR
# =========================================================
def parse_single_count(bindings: List[Dict[str, Any]]) -> int:
    if not bindings:
        return 0
    return int(float(binding_value(bindings[0], "Totale") or 0))


def parse_province(bindings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for item in bindings:
        provincia = binding_value(item, "Provincia")
        totale = binding_value(item, "Totale")
        if provincia:
            result.append(
                {
                    "Provincia": provincia,
                    "Totale": int(float(totale or 0)),
                }
            )
    return result


def parse_comuni(bindings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for item in bindings:
        comune = binding_value(item, "DescrizioneComune")
        totale = binding_value(item, "Totale")
        if comune:
            result.append(
                {
                    "DescrizioneComune": comune,
                    "Totale": int(float(totale or 0)),
                }
            )
    return result


def parse_scuole(bindings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "CodiceScuola": binding_value(item, "CodiceScuola"),
            "DenominazioneScuola": binding_value(item, "DenominazioneScuola"),
            "IndirizzoScuola": binding_value(item, "IndirizzoScuola"),
            "IndirizzoEmailScuola": binding_value(item, "IndirizzoEmailScuola"),
            "IndirizzoPecScuola": binding_value(item, "IndirizzoPecScuola"),
            "SitoWebScuola": binding_value(item, "SitoWebScuola"),
            "DescrizioneTipologiaGradoIstruzioneScuola": binding_value(
                item, "DescrizioneTipologiaGradoIstruzioneScuola"
            ),
            "DescrizioneCaratteristicaScuola": binding_value(
                item, "DescrizioneCaratteristicaScuola"
            ),
        }
        for item in bindings
    ]


def parse_search_scuole(bindings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "Provincia": binding_value(item, "Provincia"),
            "DescrizioneComune": binding_value(item, "DescrizioneComune"),
            "CodiceScuola": binding_value(item, "CodiceScuola"),
            "DenominazioneScuola": binding_value(item, "DenominazioneScuola"),
            "IndirizzoScuola": binding_value(item, "IndirizzoScuola"),
            "SitoWebScuola": binding_value(item, "SitoWebScuola"),
        }
        for item in bindings
    ]


def parse_libri(bindings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "CodiceScuola": binding_value(item, "CodiceScuola"),
            "AnnoCorso": binding_value(item, "AnnoCorso"),
            "SezioneAnno": binding_value(item, "SezioneAnno"),
            "TipoGradoScuola": binding_value(item, "TipoGradoScuola"),
            "Combinazione": binding_value(item, "Combinazione"),
            "Disciplina": binding_value(item, "Disciplina"),
            "CodiceISBN": binding_value(item, "CodiceISBN"),
            "Autori": binding_value(item, "Autori"),
            "Titolo": binding_value(item, "Titolo"),
            "Sottotitolo": binding_value(item, "Sottotitolo"),
            "Volume": binding_value(item, "Volume"),
            "Editore": binding_value(item, "Editore"),
            "Prezzo": binding_value(item, "Prezzo"),
            "NuovaAdoz": binding_value(item, "NuovaAdoz"),
            "DaAcquist": binding_value(item, "DaAcquist"),
            "Consigliato": binding_value(item, "Consigliato"),
        }
        for item in bindings
    ]


# =========================================================
# FETCHERS MIUR
# =========================================================
def fetch_province(regione: str) -> List[Dict[str, Any]]:
    endpoint = scuole_endpoint_for_regione(regione)
    cache_key = build_cache_key("province", endpoint, norm(regione))
    cached = cache_province.get(cache_key)
    if cached is not None:
        return cached

    bindings = execute_sparql(endpoint, build_province_query(regione))
    result = parse_province(bindings)
    cache_province.set(cache_key, result)
    return result


def fetch_comuni(regione: str, provincia: str) -> List[Dict[str, Any]]:
    endpoint = scuole_endpoint_for_regione(regione)
    cache_key = build_cache_key("comuni", endpoint, norm(regione), norm(provincia))
    cached = cache_comuni.get(cache_key)
    if cached is not None:
        return cached

    bindings = execute_sparql(endpoint, build_comuni_query(regione, provincia))
    result = parse_comuni(bindings)
    cache_comuni.set(cache_key, result)
    return result


def fetch_scuole(regione: str, provincia: str, comune: str, page: int, page_size: int) -> Dict[str, Any]:
    endpoint = scuole_endpoint_for_regione(regione)
    cache_key = build_cache_key(
        "scuole",
        endpoint,
        norm(regione),
        norm(provincia),
        norm(comune),
        page,
        page_size,
    )
    cached = cache_scuole.get(cache_key)
    if cached is not None:
        return cached

    offset = (page - 1) * page_size

    total_bindings = execute_sparql(
        endpoint,
        build_scuole_count_query(regione, provincia, comune),
    )
    totale = parse_single_count(total_bindings)

    row_bindings = execute_sparql(
        endpoint,
        build_scuole_query(regione, provincia, comune, page_size, offset),
    )
    scuole = parse_scuole(row_bindings)

    result = {
        "totale": totale,
        "page": page,
        "page_size": page_size,
        "has_next": offset + len(scuole) < totale,
        "scuole": scuole,
    }
    cache_scuole.set(cache_key, result)
    return result


def fetch_search_scuole(regione: str, q: str, page: int, page_size: int) -> Dict[str, Any]:
    endpoint = scuole_endpoint_for_regione(regione)
    cache_key = build_cache_key("search", endpoint, norm(regione), norm(q), page, page_size)
    cached = cache_search.get(cache_key)
    if cached is not None:
        return cached

    offset = (page - 1) * page_size

    total_bindings = execute_sparql(
        endpoint,
        build_scuole_search_count_query(regione, q),
    )
    totale = parse_single_count(total_bindings)

    row_bindings = execute_sparql(
        endpoint,
        build_scuole_search_query(regione, q, page_size, offset),
    )
    scuole = parse_search_scuole(row_bindings)

    result = {
        "totale": totale,
        "page": page,
        "page_size": page_size,
        "has_next": offset + len(scuole) < totale,
        "scuole": scuole,
    }
    cache_search.set(cache_key, result)
    return result


def fetch_libri(regione: str, codicescuola: str) -> Dict[str, Any]:
    cache_key = build_cache_key("libri", norm(regione), norm(codicescuola))
    cached = cache_libri.get(cache_key)
    if cached is not None:
        return cached

    dataset_name = ALT_DATASET_BY_REGION.get(regione)
    if not dataset_name:
        raise HTTPException(
            status_code=400,
            detail=f"Nessun dataset libri configurato per {regione}",
        )

    endpoint = f"{MIUR_OPENDATA_BASE}/{dataset_name}/query"
    all_rows: List[Dict[str, Any]] = []
    offset = 0

    while True:
        query = build_libri_query(
            codicescuola=codicescuola,
            limit=SPARQL_PAGE_SIZE,
            offset=offset,
        )

        try:
            bindings = execute_sparql(endpoint, query)
        except HTTPException as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Errore nella chiamata al servizio libri MIUR (dataset {dataset_name}): {exc.detail}",
            ) from exc

        rows = parse_libri(bindings)
        if not rows:
            break

        all_rows.extend(rows)

        if len(rows) < SPARQL_PAGE_SIZE:
            break

        offset += SPARQL_PAGE_SIZE

    result = {
        "dataset": dataset_name,
        "endpoint": endpoint,
        "libri": all_rows,
    }
    cache_libri.set(cache_key, result)
    return result


# =========================================================
# SHOPIFY REQUEST MODEL
# =========================================================
class ShopifyLibroCreateRequest(BaseModel):
    isbn: str = Field(..., min_length=3, max_length=64)
    titolo: str = Field(..., min_length=1, max_length=500)
    autore: Optional[str] = Field(default="", max_length=255)
    editore: Optional[str] = Field(default="", max_length=255)
    categoria: Optional[str] = Field(default="Libro", max_length=255)
    prezzo: Optional[float] = Field(default=0.0, ge=0)
    sottotitolo: Optional[str] = Field(default="", max_length=500)
    descrizione: Optional[str] = Field(default="", max_length=5000)
    tags: Optional[List[str]] = None


# =========================================================
# SHOPIFY HELPERS
# =========================================================
def build_minimal_shopify_product_input(payload: ShopifyLibroCreateRequest) -> Dict[str, Any]:
    isbn = payload.isbn.strip()
    titolo = payload.titolo.strip()
    editore = (payload.editore or "").strip()
    categoria = (payload.categoria or "Libro").strip()
    sottotitolo = (payload.sottotitolo or "").strip()
    descrizione = (payload.descrizione or "").strip()

    description_html_parts: List[str] = []
    if sottotitolo:
        description_html_parts.append(f"<p><strong>{html.escape(sottotitolo)}</strong></p>")
    if descrizione:
        description_html_parts.append(f"<p>{html.escape(descrizione)}</p>")

    description_html = "".join(description_html_parts) if description_html_parts else "<p></p>"

    tags = list(dict.fromkeys([t.strip() for t in (payload.tags or []) if t and t.strip()]))
    if "libro" not in [t.lower() for t in tags]:
        tags.append("libro")

    variant_input: Dict[str, Any] = {
        "sku": isbn,
        "barcode": isbn,
        "price": f"{float(payload.prezzo or 0.0):.2f}",
        "inventoryPolicy": "CONTINUE",
        "inventoryItem": {
            "tracked": False,
        },
        "optionValues": [
            {
                "optionName": "Title",
                "name": "Default Title",
            }
        ],
    }

    # Se LOCATION_ID è presente, mantieni compatibilità col tuo setup.
    if SHOPIFY_LOCATION_ID:
        variant_input["inventoryQuantities"] = [
            {
                "locationId": SHOPIFY_LOCATION_ID,
                "name": "available",
                "quantity": 0,
            }
        ]

    return {
        "title": titolo or f"Libro {isbn}",
        "descriptionHtml": description_html,
        "vendor": editore or "Editore non specificato",
        "productType": categoria or "Libro",
        "tags": tags,
        "status": "ACTIVE",
        "productOptions": [
            {
                "name": "Title",
                "position": 1,
                "values": [{"name": "Default Title"}],
            }
        ],
        "variants": [variant_input],
    }


def set_shopify_book_metafields(product_id: str, payload: ShopifyLibroCreateRequest) -> None:
    isbn = payload.isbn.strip()
    autore = (payload.autore or "").strip()
    categoria = (payload.categoria or "Libro").strip()

    variables = {
        "metafields": [
            {
                "ownerId": product_id,
                "namespace": "custom",
                "key": "autore",
                "type": "single_line_text_field",
                "value": autore or "Autore sconosciuto",
            },
            {
                "ownerId": product_id,
                "namespace": "custom",
                "key": "isbn",
                "type": "single_line_text_field",
                "value": isbn,
            },
            {
                "ownerId": product_id,
                "namespace": "custom",
                "key": "categoria",
                "type": "single_line_text_field",
                "value": categoria,
            },
            {
                "ownerId": product_id,
                "namespace": EXTERNAL_ID_NAMESPACE,
                "key": EXTERNAL_ID_KEY,
                "type": "id",
                "value": isbn,
            },
        ]
    }

    data = shopify_graphql(MUTATION_METAFIELDS_SET, variables)
    result = ((data.get("data") or {}).get("metafieldsSet")) or {}
    user_errors = result.get("userErrors") or []

    if user_errors:
        log_error("Shopify metafieldsSet userErrors", user_errors)
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Shopify metafieldsSet userErrors",
                "errors": user_errors,
            },
        )


def publish_shopify_product(product_id: str) -> None:
    if not PUBLICATION_IDS:
        return

    variables = {
        "id": product_id,
        "input": [{"publicationId": pub_id} for pub_id in PUBLICATION_IDS],
    }

    data = shopify_graphql(MUTATION_PUBLISHABLE_PUBLISH, variables)
    result = ((data.get("data") or {}).get("publishablePublish")) or {}
    user_errors = result.get("userErrors") or []

    if user_errors:
        log_error("Shopify publishablePublish userErrors", user_errors)
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Shopify publishablePublish userErrors",
                "errors": user_errors,
            },
        )


def find_shopify_product_variant_by_external_id(external_id: str) -> Optional[Dict[str, Any]]:
    cache_key = build_cache_key("shopify_lookup", norm(external_id))
    cached = cache_shopify_lookup.get(cache_key)
    if cached is not None:
        return cached

    variables = {
        "identifier": {
            "customId": {
                "namespace": EXTERNAL_ID_NAMESPACE,
                "key": EXTERNAL_ID_KEY,
                "value": external_id,
            }
        }
    }

    data = shopify_graphql(QUERY_PRODUCT_BY_CUSTOM_ID, variables)
    product = ((data.get("data") or {}).get("productByIdentifier")) or None
    if not product:
        return None

    variants = (((product.get("variants") or {}).get("nodes")) or [])
    variant = variants[0] if variants else None

    result = {
        "product_id": product.get("id"),
        "variant_id": extract_shopify_numeric_id((variant or {}).get("id")),
        "inventory_item_id": ((variant or {}).get("inventoryItem") or {}).get("id"),
        "tracked": ((variant or {}).get("inventoryItem") or {}).get("tracked"),
        "inventory_policy": (variant or {}).get("inventoryPolicy"),
    }
    cache_shopify_lookup.set(cache_key, result)
    return result


def create_minimal_shopify_product(payload: ShopifyLibroCreateRequest) -> Dict[str, Any]:
    isbn = payload.isbn.strip()

    variables = {
        "synchronous": True,
        "identifier": {
            "customId": {
                "namespace": EXTERNAL_ID_NAMESPACE,
                "key": EXTERNAL_ID_KEY,
                "value": isbn,
            }
        },
        "input": build_minimal_shopify_product_input(payload),
    }

    data = shopify_graphql(MUTATION_PRODUCT_SET_MINIMAL, variables)
    result = ((data.get("data") or {}).get("productSet")) or {}

    user_errors = result.get("userErrors") or []
    if user_errors:
        log_error("Shopify productSet userErrors", user_errors)
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Shopify productSet userErrors",
                "errors": user_errors,
            },
        )

    product = result.get("product") or {}
    product_id = product.get("id")
    variants = (((product.get("variants") or {}).get("nodes")) or [])
    variant = variants[0] if variants else None
    variant_gid = (variant or {}).get("id")

    if not product_id or not variant_gid:
        log_error("Prodotto creato ma product_id o variant_id non trovati", data)
        raise HTTPException(
            status_code=502,
            detail="Prodotto creato ma product_id o variant_id non trovati nella risposta Shopify",
        )

    set_shopify_book_metafields(product_id, payload)
    publish_shopify_product(product_id)

    created = {
        "created": True,
        "product_id": product_id,
        "variant_id": extract_shopify_numeric_id(variant_gid),
        "inventory_item_id": ((variant or {}).get("inventoryItem") or {}).get("id"),
        "tracked": ((variant or {}).get("inventoryItem") or {}).get("tracked"),
        "inventory_policy": (variant or {}).get("inventoryPolicy"),
    }

    cache_key = build_cache_key("shopify_lookup", norm(isbn))
    cache_shopify_lookup.set(cache_key, created)
    return created


# =========================================================
# API MIUR
# =========================================================
@app.get("/regioni")
def get_regioni() -> Dict[str, List[str]]:
    return {"regioni": REGIONI_CANONICHE}


@app.get("/province")
def get_province(regione: str = Query(..., max_length=100)) -> Dict[str, Any]:
    regione_norm = normalize_regione_input(regione)
    province = fetch_province(regione_norm)
    return {
        "regione": regione_norm,
        "endpoint": scuole_endpoint_for_regione(regione_norm),
        "totale": len(province),
        "province": province,
    }


@app.get("/comuni")
def get_comuni_api(
    regione: str = Query(..., max_length=100),
    provincia: str = Query(..., max_length=100),
) -> Dict[str, Any]:
    regione_norm = normalize_regione_input(regione)
    provincia_input = require_not_blank(provincia, "provincia")
    comuni = fetch_comuni(regione_norm, provincia_input)

    return {
        "regione": regione_norm,
        "endpoint": scuole_endpoint_for_regione(regione_norm),
        "provincia": provincia_input,
        "totale": len(comuni),
        "comuni": comuni,
    }


@app.get("/scuole")
def get_scuole_api(
    regione: str = Query(..., max_length=100),
    provincia: str = Query(..., max_length=100),
    comune: str = Query(..., max_length=150),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> Dict[str, Any]:
    regione_norm = normalize_regione_input(regione)
    provincia_input = require_not_blank(provincia, "provincia")
    comune_input = require_not_blank(comune, "comune")

    result = fetch_scuole(regione_norm, provincia_input, comune_input, page, page_size)
    return {
        "regione": regione_norm,
        "endpoint": scuole_endpoint_for_regione(regione_norm),
        "provincia": provincia_input,
        "comune": comune_input,
        **result,
    }


@app.get("/scuole/search")
def search_scuole_api(
    regione: str = Query(..., max_length=100),
    q: str = Query(..., min_length=2, max_length=100),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> Dict[str, Any]:
    regione_norm = normalize_regione_input(regione)
    q_input = require_not_blank(q, "q")

    if len(q_input) < 2:
        raise HTTPException(status_code=400, detail="La ricerca deve avere almeno 2 caratteri")

    result = fetch_search_scuole(regione_norm, q_input, page, page_size)
    return {
        "regione": regione_norm,
        "endpoint": scuole_endpoint_for_regione(regione_norm),
        "q": q_input,
        **result,
    }


@app.get("/libri")
def get_libri_api(
    codicescuola: str = Query(..., description="Codice scuola", max_length=32),
    regione: str = Query(..., description="Nome regione", max_length=100),
) -> Dict[str, Any]:
    codice_input = require_not_blank(codicescuola, "codicescuola")
    regione_norm = normalize_regione_input(regione)

    result = fetch_libri(
        regione=regione_norm,
        codicescuola=codice_input,
    )

    return {
        "regione": regione_norm,
        "dataset": result["dataset"],
        "endpoint": result["endpoint"],
        "codicescuola": norm(codice_input),
        "totale": len(result["libri"]),
        "libri": result["libri"],
    }


# =========================================================
# API SHOPIFY
# =========================================================
@app.post("/shopify/libri")
def create_or_get_shopify_book_api(payload: ShopifyLibroCreateRequest) -> Dict[str, Any]:
    isbn = require_not_blank(payload.isbn, "isbn")

    existing = find_shopify_product_variant_by_external_id(isbn)
    if existing and existing.get("variant_id"):
        return {
            "ok": True,
            "created": False,
            "isbn": isbn,
            "variant_id": existing["variant_id"],
            "product_id": existing.get("product_id"),
            "inventory_item_id": existing.get("inventory_item_id"),
            "tracked": existing.get("tracked"),
            "inventory_policy": existing.get("inventory_policy"),
        }

    created = create_minimal_shopify_product(payload)
    return {
        "ok": True,
        "created": True,
        "isbn": isbn,
        "variant_id": created["variant_id"],
        "product_id": created.get("product_id"),
        "inventory_item_id": created.get("inventory_item_id"),
        "tracked": created.get("tracked"),
        "inventory_policy": created.get("inventory_policy"),
    }


# =========================================================
# HEALTH
# =========================================================
@app.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}


# =========================================================
# SHUTDOWN
# =========================================================
@app.on_event("shutdown")
def shutdown_event() -> None:
    try:
        http_session.close()
    except Exception:
        pass