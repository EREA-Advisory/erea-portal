"""
EREA — Coletor de Notícias | Time de Locações
==============================================
Busca feeds RSS de portais selecionados, filtra por palavras-chave
de expansão logística/real estate, classifica relevância via Claude API
e gera o arquivo news_data.json consumido pelo portal_noticias_erea.html.

Agendamento sugerido:
  Linux/Mac : crontab -e  →  0 7 * * 1-5 /usr/bin/python3 /caminho/coletor.py
  Windows   : Task Scheduler → ação: python coletor.py, gatilho: diário 07:00
"""

import json
import hashlib
import logging
import os
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from email.utils import parsedate_to_datetime

import socket
import feedparser
import requests

# ─────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────

# Coloque sua chave Anthropic aqui OU defina a variável de ambiente ANTHROPIC_API_KEY
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "SUA_CHAVE_AQUI")

# Pasta onde este script está; o JSON e o HTML ficam ao lado dele
BASE_DIR = Path(__file__).parent
OUTPUT_JSON = BASE_DIR / "news_data.json"
LOG_FILE    = BASE_DIR / "coletor.log"

# Janela de busca: notícias das últimas N horas
HORAS_JANELA = 168  # 7 dias — aplicado a TODOS os feeds sem exceção

# Quantas notícias salvar no JSON (as mais relevantes primeiro)
MAX_NOTICIAS = 30

# Pausa entre chamadas à API Claude (segundos) — evita rate-limit
PAUSA_API = 0.5

# Timeout por feed RSS (segundos) — evita travamentos
FEED_TIMEOUT = 10

# ─────────────────────────────────────────────
# FEEDS RSS
# ─────────────────────────────────────────────

FEEDS = [
    # Geral — negócios e economia
    {"url": "https://feeds.valor.com.br/rss/empresas",          "fonte": "Valor Econômico"},
    {"url": "https://exame.com/feed/",                          "fonte": "Exame"},
    {"url": "https://www.infomoney.com.br/feed/",               "fonte": "InfoMoney"},
    {"url": "https://www.cnnbrasil.com.br/economia/feed/",      "fonte": "CNN Brasil"},

    # Logística e supply chain
    {"url": "https://www.logisticadescomplicada.com/feed/",     "fonte": "Logística Descomplicada"},
    {"url": "https://www.portosenavios.com.br/feed",            "fonte": "Portos e Navios"},
    {"url": "https://www.logisticsnews.com.br/feed/",           "fonte": "Logistics News"},
    {"url": "https://www.tiinside.com.br/feed/",                "fonte": "TI Inside Supply"},

    # Real estate e FII
    {"url": "https://www.fundsexplorer.com.br/feed",            "fonte": "Funds Explorer"},
    {"url": "https://www.buildings.com.br/feed/",               "fonte": "Buildings"},
    {"url": "https://griclub.org/feed/",                        "fonte": "GRI Club"},

    # E-commerce
    {"url": "https://www.ecommercebrasil.com.br/feed/",         "fonte": "E-commerce Brasil"},

    # Automotivo / industrial
    {"url": "https://www.automotivebusiness.com.br/feed/",      "fonte": "Automotive Business"},

    # Google News RSS por temas estratégicos
    {"url": "https://news.google.com/rss/search?q=galpão+logístico+Brasil&hl=pt-BR&gl=BR&ceid=BR:pt-419",   "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=centro+de+distribuição+expansão&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=condomínio+logístico+locação&hl=pt-BR&gl=BR&ceid=BR:pt-419",   "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=operador+logístico+novo+contrato&hl=pt-BR&gl=BR&ceid=BR:pt-419","fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=fulfillment+center+Brasil&hl=pt-BR&gl=BR&ceid=BR:pt-419",       "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=build+to+suit+galpão&hl=pt-BR&gl=BR&ceid=BR:pt-419",            "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=Mercado+Livre+armazém+CD&hl=pt-BR&gl=BR&ceid=BR:pt-419",        "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=Amazon+Brasil+logística+galpão&hl=pt-BR&gl=BR&ceid=BR:pt-419",  "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=Shopee+DHL+JSL+GXO+expansão&hl=pt-BR&gl=BR&ceid=BR:pt-419",    "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=FII+logístico+emissão+cotas&hl=pt-BR&gl=BR&ceid=BR:pt-419",    "fonte": "Google News"},

    # Portais especializados via Google News
    {"url": "https://news.google.com/rss/search?q=site:metroquadrado.com&hl=pt-BR&gl=BR&ceid=BR:pt-419",        "fonte": "Metro Quadrado"},
    {"url": "https://news.google.com/rss/search?q=site:siila.com.br&hl=pt-BR&gl=BR&ceid=BR:pt-419",             "fonte": "Siila"},
    {"url": "https://news.google.com/rss/search?q=site:mundologistica.com.br&hl=pt-BR&gl=BR&ceid=BR:pt-419",    "fonte": "Mundo Logística"},

    # Google News — empresas específicas com filtro de recência (when:7d)
    {"url": "https://news.google.com/rss/search?q=galpão+logístico+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419",              "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=centro+de+distribuição+inauguração+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=expansão+logística+brasil+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419",     "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=galpão+logístico+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419",     "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=operador+logístico+galpão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419",     "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=condomínio+logístico+locação+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419",  "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=mercado+livre+galpão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419",          "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=shopee+amazon+logística+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419",       "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=magalu+magazine+luiza+logística+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419","fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=dhl+fedex+logística+brasil+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419",    "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=fii+logístico+emissão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419",         "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=build+to+suit+galpão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419",          "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=ambev+coca-cola+unilever+galpão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419","fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=jadlog+loggi+total+express+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419",    "fonte": "Google News"},
]

# ─────────────────────────────────────────────
# PALAVRAS-CHAVE (gatilho de coleta)
# ─────────────────────────────────────────────

KEYWORDS = [
    # ── IMÓVEL LOGÍSTICO ──
    "galpão", "galpões", "armazém", "armazéns", "armazenagem",
    "condomínio logístico", "condomínios logísticos",
    "centro de distribuição", "centros de distribuição",
    "cd logístico", "cds logísticos",
    "hub logístico", "hub de distribuição", "hubs logísticos",
    "fulfillment", "fulfillment center", "dark store", "dark stores",
    "cross-docking", "crossdocking", "last mile", "last-mile",
    "ponto de distribuição", "unidade logística", "unidades logísticas",
    "imóvel logístico", "imóveis logísticos",
    "galpão industrial", "galpão frigorífico", "galpão refrigerado",

    # ── SINAIS DE EXPANSÃO — todos os tempos verbais ──
    # inaugurar
    "inaugura", "inaugurou", "inauguraram", "inauguração", "inaugurações",
    "inaugurado", "inaugurada", "inaugurar", "será inaugurado",
    # expandir
    "expande", "expandiu", "expandiram", "expansão", "expansões",
    "expandido", "expandir", "em expansão",
    # ampliar
    "amplia", "ampliou", "ampliaram", "ampliação", "ampliações",
    "ampliado", "ampliar",
    # abrir / abertura
    "abre", "abriu", "abriram", "abertura", "abrirá", "nova abertura",
    "nova unidade", "novas unidades", "nova operação", "novas operações",
    "nova fase", "novas instalações", "novo cd", "novo galpão", "novos galpões",
    # investir / investimento
    "investe", "investiu", "investiram", "investimento", "investimentos",
    "investir", "aporte",
    # contratar / contrato
    "contrata", "contratou", "contrataram", "contrato de locação",
    "contrato logístico", "locação de galpão",
    # instalar
    "instala", "instalou", "instalaram", "instalação", "instalações",
    # crescer / crescimento
    "cresce", "cresceu", "crescimento", "crescer",
    # construir / construção
    "constrói", "construiu", "construção", "build to suit", "bts logístico",
    # implantar
    "implanta", "implantou", "implantação",
    # mudar / transferir (mudança de endereço = novo galpão)
    "transfere operação", "transferiu operação", "nova sede logística",

    # ── FINANCEIRO ──
    "sale and leaseback", "fii logístico", "fundo logístico",
    "emissão de cotas", "cri logístico", "captação logística",
    "aporte logístico", "investimento logístico",

    # ── SETOR / MERCADO ──
    "operador logístico", "3pl", "supply chain brasil",
    "e-commerce logística", "omnichannel", "real estate logístico",
    "cadeia de abastecimento", "cadeia logística", "operação logística",

    # ── EMPRESAS MONITORADAS ──
    # E-commerce / varejo
    "mercado livre", "mercadolivre",
    "shopee",
    "amazon brasil", "amazon logística",
    "magazine luiza", "magalu",
    "via varejo", "casas bahia", "ponto frio",
    "b2w", "americanas", "americanas s.a",
    "shein",
    "dafiti",
    "netshoes", "grupo netshoes",
    "madeira madeira", "madeiramadeira",
    "fast shop", "fastshop",
    "tok stok", "tok&stok",
    "lojas renner", "renner",
    "riachuelo",
    "c&a brasil", "c&a",
    "pernambucanas",
    "grupo mateus",
    "kalunga",
    "lojas colombo",
    "lojas lebes",
    "lojas leader",
    "centauro", "grupo sbf",
    "arezzo", "arezzo&co",
    "zara brasil", "zara",
    "decathlon brasil", "decathlon",
    "westwing",
    "mobly",
    "grupo muffato",
    "atacadão",
    "assaí", "assai atacadista",
    "grupo zaffari",
    "supermercado lopes",
    "o amigão",
    "chama supermercados",
    "comercial esperança",
    "obramax",

    # Logística / transporte
    "dhl", "dhl supply chain", "dhl express",
    "fedex", "fedex express",
    "ups brasil",
    "correios",
    "jadlog",
    "total express",
    "loggi",
    "azul cargo", "azul linhas aéreas",
    "jamef",
    "braspress",
    "patrus transportes",
    "transportadora plimor", "plimor",
    "expresso 3300",
    "transbuiatte",
    "transportadora minuano", "minuano",
    "sigma transportes",
    "unilog express",
    "j&t express", "j&t brasil",
    "anjun brasil",
    "movvi logística", "movvi",
    "modular cargas",
    "transrapido",
    "master cargas",
    "postall log",
    "brasmundi logística",
    "gat logística",
    "ellece logística",
    "renovação logística",
    "ativa logística",
    "smart logística",
    "intecom logística",
    "fivelog",
    "ziran logística",
    "comando log",
    "fitlogística",
    "vtc operador logístico",
    "mr3 operador logístico",
    "osten group",
    "supporte full commerce",
    "vendemmia logística",
    "belenus",

    # 3PL / operadores especializados
    "luft logistics",
    "id logistics", "id logístics",
    "ceva logistics",
    "kuehne nagel", "kuehne+nagel",
    "dsv", "dsv air sea",
    "fm logistic",
    "martin brower",
    "maersk logística", "maersk",
    "yusen logistics",
    "andreani",
    "bomi group",
    "celistics",
    "infracommerce",
    "multilog",
    "grupo tecadi", "tecadi",
    "unidão transportes",
    "mundial logistics",
    "comfrio",
    "friozem",
    "superfrio logística",
    "arfrio",
    "frigelar",
    "grupo friopeças",

    # Indústria / manufatura
    "ambev",
    "coca-cola femsa", "coca cola femsa", "femsa",
    "heineken brasil", "heineken",
    "pepsico brasil", "pepsico",
    "bauducco",
    "wickbold",
    "m. dias branco", "m dias branco",
    "fini brasil",
    "brf",
    "seara alimentos", "seara",
    "jbs",
    "natura",
    "grupo boticário", "boticário", "boticario",
    "l'oréal brasil", "loreal brasil", "l oreal",
    "unilever brasil", "unilever",
    "colgate palmolive", "colgate-palmolive",
    "sanofi brasil", "sanofi",
    "reckitt brasil", "reckitt",
    "hypera farmacêutica", "hypera",
    "eurofarma",
    "davene",
    "premierPet", "premieRpet",
    "ypê", "ype",
    "whirlpool",
    "electrolux",
    "midea carrier", "midea",
    "britânia eletrodomésticos", "britania",
    "samsung brasil", "samsung",
    "lenovo brasil", "lenovo",
    "foxconn brasil", "foxconn",
    "semp tcl", "semp",
    "elgin",
    "bosch",
    "mercedes benz brasil", "mercedes-benz",
    "ford motor brasil", "ford brasil",
    "gm brasil", "general motors brasil",
    "renault nissan", "renault brasil",
    "volkswagen brasil",
    "agco brasil",
    "john deere brasil", "john deere",
    "embraer",
    "braskem",
    "suzano",
    "bridgestone brasil", "bridgestone",
    "benteler brasil", "benteler",
    "plascar",
    "marelli",
    "cummins brasil", "cummins",
    "ericsson brasil", "ericsson",
    "ascenty",
    "scala data centers", "scala",
    "iron mountain brasil", "iron mountain",
    "sealed air",
    "skf brasil",
    "assa abloy",
    "galderma",
    "ingredient incorporated", "ingredion",
    "ontex brasil",
    "dorel juvenile",
    "cal-comp",
    "yangzi brasil",
    "grupo seb",
    "grupo dpsp", "drogaria são paulo",
    "rd raia drogasil", "raia drogasil", "rd saúde",
    "grupo belmicro", "belmicro",
    "petlove",
    "viveo",
    "tpc logística", "tpc",
    "grupo martins", "martins",
    "mcassab",
    "ascensus",
    "cantu pneus",
    "rojemac",
    "mtc log",
    "nagumo",
    "fortgreen fertilizantes",
    "zaraplast",
    "plasnox",
    "grupo mirassol",
    "grupo toniato",
    "grupo pegoraro",
    "dufrio",
    "embare",
    "unicharm",
    "interbrands",
    "fotus distribuidora",
    "somos educação",
    "gpa brasil", "gpa",
    "grupo sc",
    "petz",
    "localiza",
    "lojas caedu", "caedu",
    "gtex",
    "obramax",
    "multi armazéns",
    "sca hygiene",
    "caedú",
    "vonder",
    "grupo ovd",
    "eixo snetor",
    "grupo 3 corações", "3 corações",
    "grupo muffato",
]

# Categorias para classificação
CATEGORIAS = {
    "expansao":    ["expansão", "nova unidade", "inauguração", "ampliação", "novo cd", "novo galpão", "novas instalações", "nova operação"],
    "logistica":   ["logística", "armazém", "operador logístico", "3pl", "supply chain", "cross-docking", "last mile", "fulfillment"],
    "investimento":["fii", "fundo", "captação", "aporte", "emissão", "sale and leaseback", "cri", "build to suit"],
    "ecommerce":   ["e-commerce", "mercado livre", "shopee", "amazon", "marketplace", "gmv", "omnichannel"],
    "industrial":  ["industrial", "galpão industrial", "ilg", "automotivo", "montadora", "fornecedor"],
}

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

# Abreviações de estados para deduplicação
_ABREV = {"sp":"sao paulo","rj":"rio de janeiro","mg":"minas gerais","pr":"parana",
          "sc":"santa catarina","rs":"rio grande do sul","ba":"bahia","ce":"ceara",
          "go":"goias","pe":"pernambuco","am":"amazonas","pa":"para","ma":"maranhao",
          "es":"espirito santo","mt":"mato grosso","ms":"mato grosso do sul",
          "df":"distrito federal","to":"tocantins","pi":"piaui","rn":"rio grande norte"}

_STOPWORDS_DEDUP = {"e","o","a","os","as","de","do","da","dos","das","em","no","na",
                    "nos","nas","com","por","para","que","um","uma","ao","aos","se",
                    "mais","seu","sua","novo","nova","novos","novas","grande","gigante",
                    "trabalhe","empresa","presenca","brasil","grupo","seus","suas",
                    "industrial","logistica","operacao","regional","mercado"}

def _tokens(titulo: str) -> set:
    titulo = re.sub(r" [-|] .{3,50}$", "", titulo)
    titulo = re.sub(r"[|].{2,50}$", "", titulo)
    import unicodedata
    titulo = unicodedata.normalize("NFD", titulo)
    titulo = "".join(c for c in titulo if unicodedata.category(c) != "Mn")
    titulo = re.sub(r"[^\w\s]", " ", titulo).lower()
    palavras = titulo.split()
    # Expande abreviações
    palavras = [_ABREV.get(p, p) for p in palavras]
    return set(p for p in palavras if len(p) > 2 and p not in _STOPWORDS_DEDUP)

def _jaccard(t1: str, t2: str) -> float:
    s1, s2 = _tokens(t1), _tokens(t2)
    if not s1 or not s2: return 0.0
    return len(s1 & s2) / len(s1 | s2)

def _normalizar_titulo(titulo: str) -> str:
    titulo = re.sub(r" [-|] .{3,50}$", "", titulo.strip())
    titulo = re.sub(r"[|].{2,50}$", "", titulo.strip())
    import unicodedata
    titulo = unicodedata.normalize("NFD", titulo)
    titulo = "".join(c for c in titulo if unicodedata.category(c) != "Mn")
    titulo = re.sub(r"[^\w\s]", " ", titulo).lower()
    return re.sub(r"\s+", " ", titulo).strip()

def _id(titulo: str) -> str:
    """ID estável baseado no título normalizado."""
    return hashlib.md5(_normalizar_titulo(titulo).encode()).hexdigest()[:10]

def _deduplicar(candidatas: list) -> list:
    """Remove notícias similares (Jaccard >= 0.35) mantendo a de maior score."""
    THRESHOLD = 0.35
    resultado = []
    for nova in candidatas:
        similar = False
        for existente in resultado:
            if _jaccard(nova["headline"], existente["headline"]) >= THRESHOLD:
                # Mantém a que tiver mais keywords (score maior)
                if nova.get("score", 0) > existente.get("score", 0):
                    resultado.remove(existente)
                    resultado.append(nova)
                similar = True
                break
        if not similar:
            resultado.append(nova)
    log.info(f"Após deduplicação por similaridade: {len(resultado)} notícias ({len(candidatas)-len(resultado)} removidas)")
    return resultado


def _parse_data(entry) -> datetime | None:
    """Extrai datetime da entrada RSS; retorna None se falhar.
    Tenta published_parsed / updated_parsed (structs já parseadas pelo feedparser)
    antes de fazer o parse manual da string — mais robusto.
    """
    # Tenta structs pré-parseadas pelo feedparser (mais confiáveis)
    for campo in ("published_parsed", "updated_parsed"):
        val = getattr(entry, campo, None)
        if val:
            try:
                return datetime(*val[:6], tzinfo=timezone.utc)
            except Exception:
                pass
    # Fallback: parse manual da string
    for campo in ("published", "updated"):
        val = getattr(entry, campo, None)
        if val:
            try:
                return parsedate_to_datetime(val).astimezone(timezone.utc)
            except Exception:
                pass
    return None


def _formata_tempo(dt: datetime) -> str:
    """Formata data relativa. dt é sempre válido (entradas sem data são descartadas)."""
    agora = datetime.now(timezone.utc)
    diff  = agora - dt
    minutos = int(diff.total_seconds() / 60)
    if minutos < 60:
        return f"há {minutos} min"
    horas = int(diff.total_seconds() / 3600)
    if horas < 24:
        return f"hoje, {dt.astimezone().strftime('%H:%M')}"
    if horas < 48:
        return f"ontem, {dt.astimezone().strftime('%H:%M')}"
    # Não deve chegar aqui (entradas >24h são descartadas), mas por segurança:
    return dt.astimezone().strftime("%d/%m às %H:%M")


def _texto_completo(entry) -> str:
    titulo  = getattr(entry, "title", "") or ""
    summary = getattr(entry, "summary", "") or ""
    # Remove tags HTML simples do summary
    summary = re.sub(r"<[^>]+>", " ", summary)
    return f"{titulo}. {summary}".lower()


def _contar_keywords(texto: str) -> int:
    return sum(1 for kw in KEYWORDS if kw in texto)

def _tem_keyword(texto: str) -> bool:
    """Exige pelo menos 2 keywords para reduzir ruído."""
    return _contar_keywords(texto) >= 2


def _extrair_link(entry) -> str:
    """Retorna o URL real da notícia.
    Para Google News, usa decode base64 do CBMi... para obter o link original.
    """
    import base64
    from urllib.parse import urlparse, parse_qs, unquote

    raw_link = getattr(entry, "link", "") or ""

    # Estratégia 1: entry.links com rel=alternate
    for lnk in getattr(entry, "links", []):
        href = lnk.get("href", "")
        rel  = lnk.get("rel", "")
        if href and "news.google.com" not in href and rel == "alternate":
            return href

    # Estratégia 2: decode base64 do path do Google News (CBMi...)
    if "news.google.com" in raw_link and "articles/" in raw_link:
        try:
            part = raw_link.split("articles/")[-1].split("?")[0]
            padded = part + "=" * (4 - len(part) % 4)
            decoded = base64.urlsafe_b64decode(padded)
            urls = re.findall(rb"https?://[^\x00-\x1f\x7f\s<>]+", decoded)
            if urls:
                candidate = urls[0].decode("utf-8", errors="ignore").rstrip(".,;)")
                # Valida que não é homepage (tem path com pelo menos 2 segmentos)
                parsed_c = urlparse(candidate)
                if len(parsed_c.path.strip("/").split("/")) >= 1 and len(parsed_c.path) > 3:
                    return candidate
        except Exception:
            pass

    # Estratégia 3: parâmetro ?url= na query string
    if "news.google.com" in raw_link:
        parsed_q = urlparse(raw_link)
        qs = parse_qs(parsed_q.query)
        if "url" in qs:
            return unquote(qs["url"][0])

    # Estratégia 4: href no summary HTML
    summary_raw = getattr(entry, "summary", "") or ""
    match = re.search(r'href="(https?://(?!news\.google)[^"]+)"', summary_raw)
    if match:
        candidate = match.group(1)
        parsed_c = urlparse(candidate)
        if len(parsed_c.path) > 3:
            return candidate

    # Estratégia 5: source.href
    source_href = getattr(getattr(entry, "source", None), "href", None)
    if source_href and "news.google.com" not in source_href:
        return source_href

    return raw_link if raw_link else "#"




def _categoria(texto: str) -> str:
    scores = {cat: 0 for cat in CATEGORIAS}
    for cat, termos in CATEGORIAS.items():
        for t in termos:
            if t in texto:
                scores[cat] += 1
    melhor = max(scores, key=scores.get)
    return melhor if scores[melhor] > 0 else "logistica"

# ─────────────────────────────────────────────
# COLETA RSS
# ─────────────────────────────────────────────

def coletar_feeds(horas: int) -> list[dict]:
    """Percorre todos os feeds e retorna entradas dentro da janela de tempo."""
    corte = datetime.now(timezone.utc) - timedelta(hours=horas)
    log.info(f"Janela de busca: ultimas {horas}h (corte UTC: {corte.strftime('%Y-%m-%d %H:%M')})")
    candidatas = []
    ids_vistos  = set()

    socket.setdefaulttimeout(FEED_TIMEOUT)

    for feed_cfg in FEEDS:
        url       = feed_cfg["url"]
        fonte     = feed_cfg["fonte"]
        log.info(f"Buscando: {fonte} — {url[:70]}…")
        try:
            # Busca o XML bruto para preservar os links CBMi do Google News
            # antes que o feedparser os resolva/corrompa
            raw_xml = None
            if "news.google.com" in url:
                try:
                    import urllib.request as _ur
                    req = _ur.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible)"})
                    raw_xml = _ur.urlopen(req, timeout=FEED_TIMEOUT).read()
                except Exception:
                    pass
            parsed = feedparser.parse(raw_xml or url,
                                      request_headers={"User-Agent": "Mozilla/5.0 (compatible)"})
        except Exception as e:
            log.warning(f"  Erro ao parsear {fonte}: {e}")
            continue

        total_entradas = len(parsed.entries)
        sem_data = 0
        fora_janela = 0
        sem_keyword = 0
        aceitas = 0

        for entry in parsed.entries:
            dt = _parse_data(entry)

            if dt is None:
                sem_data += 1
                continue

            if dt < corte:
                fora_janela += 1
                continue

            titulo = getattr(entry, "title", "").strip()
            if not titulo:
                continue

            uid = _id(titulo)
            if uid in ids_vistos:
                continue
            ids_vistos.add(uid)

            texto = _texto_completo(entry)
            if not _tem_keyword(texto):
                sem_keyword += 1
                continue

            link_real = _extrair_link(entry)
            aceitas += 1
            log.info(f"  ACEITA: {titulo[:80]}")

            candidatas.append({
                "id":       uid,
                "headline": titulo,
                "source":   fonte,
                "link":     link_real,
                "time":     _formata_tempo(dt),
                "dt_iso":   dt.isoformat(),
                "summary":  re.sub(r"<[^>]+>", " ", getattr(entry, "summary", "") or "").strip()[:600],
                "category": _categoria(texto),
            })

        log.info(f"  {fonte}: {total_entradas} entradas | sem data: {sem_data} | fora janela: {fora_janela} | sem keyword: {sem_keyword} | aceitas: {aceitas}")

    log.info(f"TOTAL candidatas: {len(candidatas)}")
    return candidatas


# ─────────────────────────────────────────────
# ANÁLISE VIA CLAUDE API
# ─────────────────────────────────────────────

SYSTEM_PROMPT = """Você é analista sênior de Real Estate logístico da EREA Advisory, especializado em identificar oportunidades de locação de galpões e imóveis industriais no Brasil.

Dado o título e resumo de uma notícia, você deve:
1. Avaliar se a notícia representa um sinal real de demanda por imóvel logístico (expansão, novo CD, novo hub, captação para crescimento, etc.)
2. Atribuir um score de relevância de 0 a 10 (10 = oportunidade imediata de locação)
3. Escrever UMA frase curta explicando POR QUE é relevante para o time de locações — seja específico sobre o tipo de imóvel, região ou ação recomendada

Responda APENAS com JSON válido, sem markdown, sem texto fora do JSON:
{
  "score": <0-10>,
  "relevance": "<frase de até 180 caracteres explicando a relevância para locações>",
  "badges": ["<badge1>","<badge2>"]
}

Badges disponíveis: "Destaque", "Expansão", "Logística", "Investimento", "E-commerce", "Industrial"
Use "Destaque" apenas se score >= 7.
Use no máximo 2 badges."""


def _badges_por_categoria(categoria: str) -> list:
    return {
        "expansao":    ["Expansão"],
        "logistica":   ["Logística"],
        "investimento":["Investimento"],
        "ecommerce":   ["E-commerce"],
        "industrial":  ["Industrial"],
    }.get(categoria, ["Logística"])


def _relevance_automatica(noticia: dict) -> str:
    """Gera texto de relevância baseado em regras simples quando Claude não está disponível."""
    texto = (noticia.get("headline","") + " " + noticia.get("summary","")).lower()
    hints = []
    if any(t in texto for t in ["build to suit", "bts"]):
        hints.append("Oportunidade de desenvolvimento BTS.")
    if any(t in texto for t in ["sale and leaseback"]):
        hints.append("Potencial de sale & leaseback.")
    if any(t in texto for t in ["3pl", "operador logístico"]):
        hints.append("Operador 3PL — potencial demanda de galpão.")
    if any(t in texto for t in ["centro de distribuição", "cd logístico", "fulfillment"]):
        hints.append("Expansão de CD — demanda direta por imóvel logístico.")
    if any(t in texto for t in ["fii", "fundo", "emissão"]):
        hints.append("Movimento de capital — possível apetite por aquisição de ativos.")
    if any(t in texto for t in ["expansão", "nova unidade", "inauguração"]):
        hints.append("Sinal de expansão — prospecção ativa recomendada.")
    return " ".join(hints) if hints else "Notícia relevante para o mercado logístico."


def analisar_com_claude(noticia: dict) -> dict:
    """Classificação automática por regras — sem chamada à API."""
    cat    = noticia.get("category", "logistica")
    badges = _badges_por_categoria(cat)
    texto  = (noticia.get("headline","") + " " + noticia.get("summary","")).lower()
    kw_count = _contar_keywords(texto)
    # Score proporcional ao número de keywords (mín 5, máx 9)
    score = min(9, 4 + kw_count)
    # Muitas keywords = badge Destaque
    if kw_count >= 4:
        badges = ["Destaque"] + [b for b in badges if b != "Destaque"]
    log.info(f"  keywords={kw_count} score={score} cat={cat}")
    return {
        **noticia,
        "score":    score,
        "relevance": _relevance_automatica(noticia),
        "badges":   badges,
    }


# ─────────────────────────────────────────────
# GERAÇÃO DO JSON
# ─────────────────────────────────────────────

def gerar_json(noticias: list[dict]) -> None:
    """Salva o news_data.json consumido pelo portal HTML.
    Aplica corte de data como última linha de defesa antes de salvar.
    """
    corte_final = datetime.now(timezone.utc) - timedelta(hours=HORAS_JANELA)
    filtradas = []
    for n in noticias:
        dt_iso = n.get("dt_iso")
        if dt_iso:
            try:
                dt = datetime.fromisoformat(dt_iso)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if dt < corte_final:
                    log.warning(f"  BLOQUEADA na validação final (data antiga): [{dt.strftime('%d/%m/%Y')}] {n['headline'][:60]}")
                    continue
            except Exception:
                pass
        filtradas.append(n)

    removidas = len(noticias) - len(filtradas)
    if removidas:
        log.warning(f"  {removidas} notícia(s) removida(s) por data anterior a {corte_final.strftime('%d/%m/%Y')} na validação final.")

    payload = {
        "gerado_em": datetime.now().strftime("%d/%m/%Y às %H:%M"),
        "total": len(filtradas),
        "noticias": filtradas,
    }
    OUTPUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(f"JSON salvo em: {OUTPUT_JSON} ({len(filtradas)} notícias)")



# ─────────────────────────────────────────────
# RESOLUÇÃO DE LINK E EXTRAÇÃO DE CONTEÚDO
# ─────────────────────────────────────────────

def _resolver_link(url: str) -> str:
    """Segue redirects HTTP para obter o URL final da notícia.
    Resolve links do Google News que ainda apontam para o agregador.
    """
    if not url or url == "#":
        return url
    # Se já é um link direto (não Google News), retorna como está
    if "news.google.com" not in url:
        return url
    try:
        resp = requests.get(
            url,
            allow_redirects=True,
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (compatible)"},
        )
        final = resp.url
        # Valida que chegamos numa notícia, não numa homepage
        from urllib.parse import urlparse
        parsed = urlparse(final)
        if len(parsed.path.strip("/")) > 5:
            log.info(f"    Link resolvido: {final[:80]}")
            return final
    except Exception as e:
        log.debug(f"    _resolver_link falhou: {e}")
    return url


def _extrair_conteudo(url: str, titulo: str) -> str:
    """Faz scraping do conteúdo textual da notícia a partir do URL.
    Retorna os primeiros ~1500 caracteres do corpo da matéria.
    """
    if not url or url == "#" or "news.google.com" in url:
        return ""
    try:
        resp = requests.get(
            url,
            timeout=12,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "pt-BR,pt;q=0.9",
            },
        )
        resp.raise_for_status()
        html = resp.text

        # Remove scripts, styles, nav, footer, header
        html = re.sub(r'<(script|style|nav|footer|header|aside|form)[^>]*>.*?</\1>', ' ', html, flags=re.DOTALL|re.IGNORECASE)
        # Remove todas as tags HTML
        texto = re.sub(r'<[^>]+>', ' ', html)
        # Limpa espaços e entidades HTML
        texto = re.sub(r'&[a-z]+;', ' ', texto)
        texto = re.sub(r'\s+', ' ', texto).strip()

        # Tenta localizar o início do conteúdo da notícia
        # procura pelo título no texto e pega o que vem depois
        titulo_limpo = re.sub(r'[^\w\s]', '', titulo.lower())[:40]
        palavras_titulo = titulo_limpo.split()[:4]
        padrao = ''.join(p + r'[\s\S]{0,20}' for p in palavras_titulo[:3])
        match = re.search(padrao, texto, re.IGNORECASE)
        if match:
            inicio = match.start()
            conteudo = texto[inicio:inicio + 6000]
        else:
            # Fallback: pega o meio do texto (evita menu/header)
            meio = len(texto) // 4
            conteudo = texto[meio:meio + 6000]

        # Limpa e retorna
        conteudo = re.sub(r'\s+', ' ', conteudo).strip()
        return conteudo[:1500]

    except Exception as e:
        log.debug(f"    _extrair_conteudo falhou para {url[:60]}: {e}")
        return ""

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("EREA Coletor iniciado")
    log.info("=" * 60)

    # 1. Coletar candidatas dos feeds
    candidatas = coletar_feeds(HORAS_JANELA)

    if not candidatas:
        log.warning("Nenhuma notícia encontrada. Verifique os feeds e as keywords.")
        gerar_json([])
        return

    # 2. Deduplicar por similaridade de título
    candidatas = _deduplicar(candidatas)

    # 3. Analisar (score + relevância)
    log.info(f"Analisando {len(candidatas)} notícias…")
    enriquecidas = []
    for i, n in enumerate(candidatas, 1):
        log.info(f"  [{i}/{len(candidatas)}] {n['headline'][:70]}…")
        resultado = analisar_com_claude(n)
        enriquecidas.append(resultado)
        time.sleep(PAUSA_API)

    # 4. Ordenar por score e pegar as top N
    enriquecidas.sort(key=lambda x: x["score"], reverse=True)
    top = enriquecidas[:MAX_NOTICIAS]

    # 5. Resolver links e extrair conteúdo completo
    log.info("Resolvendo links e extraindo conteúdo das notícias…")
    for i, n in enumerate(top, 1):
        log.info(f"  [{i}/{len(top)}] {n['headline'][:60]}…")
        # Resolve o link final (segue redirect do Google News)
        link_resolvido = _resolver_link(n["link"])
        n["link"] = link_resolvido
        # Extrai conteúdo completo da página
        conteudo = _extrair_conteudo(link_resolvido, n["headline"])
        n["conteudo"] = conteudo
        if conteudo:
            log.info(f"    Conteúdo: {len(conteudo)} chars")
        else:
            log.info(f"    Conteúdo: não disponível")
        time.sleep(0.5)

    log.info(f"Top {len(top)} notícias selecionadas (score mín: {top[-1]['score'] if top else '-'})")

    # 6. Salvar JSON
    gerar_json(top)
    log.info("Coleta concluída com sucesso.")


if __name__ == "__main__":
    main()