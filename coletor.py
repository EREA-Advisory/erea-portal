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
HORAS_JANELA = 24

# Quantas notícias salvar no JSON (as mais relevantes primeiro)
MAX_NOTICIAS = 20

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
]

# ─────────────────────────────────────────────
# PALAVRAS-CHAVE (gatilho de coleta)
# ─────────────────────────────────────────────

KEYWORDS = [
    # Imóvel logístico
    "galpão", "armazém", "condomínio logístico", "centro de distribuição",
    "cd logístico", "hub logístico", "fulfillment", "dark store",
    "cross-docking", "last mile", "last-mile",

    # Sinais de expansão
    "expansão logística", "nova operação", "nova unidade", "inauguração",
    "ampliação", "novo cd", "novo galpão", "contrato de locação",
    "sale and leaseback", "build to suit", "bts logístico",
    "nova fase", "novas instalações",

    # Financeiro / capital
    "aporte logística", "captação logística", "fii logístico",
    "fundo logístico", "emissão de cotas", "cri logístico",

    # Players
    "mercado livre logística", "shopee galpão", "amazon brasil armazém",
    "sequoia logística", "jsl logística", "tegma", "gxo",
    "dhl supply chain", "localfrio", "3pl brasil",

    # Setor / mercado
    "e-commerce logística", "omnichannel armazém", "supply chain brasil",
    "operador logístico", "imóvel logístico", "real estate logístico",
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

def _id(titulo: str) -> str:
    """ID estável baseado no título (evita duplicatas)."""
    return hashlib.md5(titulo.lower().encode()).hexdigest()[:10]


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


def _tem_keyword(texto: str) -> bool:
    return any(kw in texto for kw in KEYWORDS)


def _extrair_link(entry) -> str:
    """Retorna o URL real da notícia, desencapsulando redirects do Google News.

    O Google News envolve cada link num redirect:
      https://news.google.com/rss/articles/CBMi...
    O link original fica em dois lugares acessíveis sem fazer HTTP request:
      1. entry.links[].href com rel='alternate'  (nem sempre presente)
      2. Parâmetro ?url= dentro do summary HTML   (nem sempre presente)
      3. Fallback: retorna o link do Google News mesmo (pelo menos abre a notícia)
    """
    raw_link = getattr(entry, "link", "#") or "#"

    # Tenta extrair de entry.links (rel=alternate)
    for lnk in getattr(entry, "links", []):
        href = lnk.get("href", "")
        if href and "news.google.com" not in href:
            return href

    # Tenta extrair parâmetro url= do summary (alguns feeds embeddm o link)
    summary_raw = getattr(entry, "summary", "") or ""
    match = re.search(r'href="(https?://(?!news\.google)[^"]+)"', summary_raw)
    if match:
        return match.group(1)

    # O feedparser às vezes popula source.href com o link real
    source_href = getattr(getattr(entry, "source", None), "href", None)
    if source_href and "news.google.com" not in source_href:
        return source_href

    return raw_link  # fallback: link do Google News (ainda abre a notícia)




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
        url    = feed_cfg["url"]
        fonte  = feed_cfg["fonte"]
        log.info(f"Buscando: {fonte} — {url[:70]}…")
        try:
            parsed = feedparser.parse(url, request_headers={'User-Agent': 'Mozilla/5.0 (compatible)'})
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


def analisar_com_claude(noticia: dict) -> dict:
    """Chama a API Claude para enriquecer a notícia com score e análise."""
    if ANTHROPIC_API_KEY == "SUA_CHAVE_AQUI":
        log.warning("Chave API não configurada — pulando análise Claude.")
        return {**noticia, "score": 5, "relevance": "Configure ANTHROPIC_API_KEY para análise automática.", "badges": []}

    prompt = f"""Título: {noticia['headline']}

Resumo: {noticia['summary']}

Fonte: {noticia['source']}"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 300,
                "system": SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=20,
        )
        resp.raise_for_status()
        raw = resp.json()["content"][0]["text"].strip()
        parsed = json.loads(raw)
        return {
            **noticia,
            "score":    int(parsed.get("score", 5)),
            "relevance": parsed.get("relevance", ""),
            "badges":   parsed.get("badges", []),
        }
    except Exception as e:
        log.warning(f"Erro Claude para '{noticia['headline'][:60]}': {e}")
        return {**noticia, "score": 5, "relevance": "Análise indisponível.", "badges": []}


# ─────────────────────────────────────────────
# GERAÇÃO DO JSON
# ─────────────────────────────────────────────

def gerar_json(noticias: list[dict]) -> None:
    """Salva o news_data.json consumido pelo portal HTML."""
    payload = {
        "gerado_em": datetime.now().strftime("%d/%m/%Y às %H:%M"),
        "total": len(noticias),
        "noticias": noticias,
    }
    OUTPUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(f"JSON salvo em: {OUTPUT_JSON} ({len(noticias)} notícias)")


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

    # 2. Analisar com Claude (score + relevância)
    log.info(f"Analisando {len(candidatas)} notícias com Claude…")
    enriquecidas = []
    for i, n in enumerate(candidatas, 1):
        log.info(f"  [{i}/{len(candidatas)}] {n['headline'][:70]}…")
        resultado = analisar_com_claude(n)
        enriquecidas.append(resultado)
        time.sleep(PAUSA_API)

    # 3. Ordenar por score e pegar as top N
    enriquecidas.sort(key=lambda x: x["score"], reverse=True)
    top = enriquecidas[:MAX_NOTICIAS]

    log.info(f"Top {len(top)} notícias selecionadas (score mín: {top[-1]['score'] if top else '-'})")

    # 4. Salvar JSON
    gerar_json(top)
    log.info("Coleta concluída com sucesso.")


if __name__ == "__main__":
    main()
