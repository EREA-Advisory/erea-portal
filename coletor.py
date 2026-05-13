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

BRT = timezone(timedelta(hours=-3))  # Horário de Brasília
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
MAX_NOTICIAS = 50

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
    {"url": "https://mundologistica.com.br/feed",                                                                     "fonte": "Mundo Logística"},
    {"url": "https://mundologistica.com.br/rss",                                                                      "fonte": "Mundo Logística"},

    # Portais adicionais via RSS direto
    {"url": "https://www.modaisemfoco.com.br/feed/",  "fonte": "Modais em Foco"},
    {"url": "https://www.modaisemfoco.com.br/rss/",   "fonte": "Modais em Foco"},

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
    {"url": "https://news.google.com/rss/search?q=total+express+fulfillment+operação+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},
    {"url": "https://news.google.com/rss/search?q=site:modaisemfoco.com.br+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419",           "fonte": "Modais em Foco"},
    {"url": "https://news.google.com/rss/search?q=site:mundologistica.com.br+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419",         "fonte": "Mundo Logística"},
    {"url": "https://news.google.com/rss/search?q=site:portosenavios.com.br+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419",          "fonte": "Portos e Navios"},
    {"url": "https://news.google.com/rss/search?q=site:logisticsnews.com.br+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419",          "fonte": "Logistics News"},

    # Google News — busca dedicada por empresa monitorada
    {"url": "https://news.google.com/rss/search?q=Mercado+Livre+Amazon+Brasil+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Mercado Livre | Amazon Brasil
    {"url": "https://news.google.com/rss/search?q=Shopee+Shein+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Shopee | Shein
    {"url": "https://news.google.com/rss/search?q=Magazine+Luiza+Magalu+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Magazine Luiza | Magalu
    {"url": "https://news.google.com/rss/search?q=Via+Varejo+Casas+Bahia+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Via Varejo | Casas Bahia
    {"url": "https://news.google.com/rss/search?q=Americanas+B2W+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Americanas | B2W
    {"url": "https://news.google.com/rss/search?q=Dafiti+Netshoes+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Dafiti | Netshoes
    {"url": "https://news.google.com/rss/search?q=Assaí+Atacadista+Atacadão+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Assaí Atacadista | Atacadão
    {"url": "https://news.google.com/rss/search?q=Lojas+Renner+Riachuelo+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Lojas Renner | Riachuelo
    {"url": "https://news.google.com/rss/search?q=C&A+Brasil+Pernambucanas+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # C&A Brasil | Pernambucanas
    {"url": "https://news.google.com/rss/search?q=Grupo+Mateus+GPA+Brasil+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Grupo Mateus | GPA Brasil
    {"url": "https://news.google.com/rss/search?q=DHL+Supply+Chain+FedEx+Brasil+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # DHL Supply Chain | FedEx Brasil
    {"url": "https://news.google.com/rss/search?q=Jadlog+Total+Express+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Jadlog | Total Express
    {"url": "https://news.google.com/rss/search?q=Loggi+Azul+Cargo+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Loggi | Azul Cargo
    {"url": "https://news.google.com/rss/search?q=JSL+Logística+Sequoia+Logística+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # JSL Logística | Sequoia Logística
    {"url": "https://news.google.com/rss/search?q=Luft+Logistics+ID+Logistics+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Luft Logistics | ID Logistics
    {"url": "https://news.google.com/rss/search?q=CEVA+Logistics+Kuehne+Nagel+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # CEVA Logistics | Kuehne Nagel
    {"url": "https://news.google.com/rss/search?q=DSV+Logística+FM+Logistic+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # DSV Logística | FM Logistic
    {"url": "https://news.google.com/rss/search?q=Maersk+Brasil+Yusen+Logistics+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Maersk Brasil | Yusen Logistics
    {"url": "https://news.google.com/rss/search?q=Multilog+Grupo+Tecadi+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Multilog | Grupo Tecadi
    {"url": "https://news.google.com/rss/search?q=Comfrio+SuperFrio+Logística+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Comfrio | SuperFrio Logística
    {"url": "https://news.google.com/rss/search?q=Ambev+Heineken+Brasil+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Ambev | Heineken Brasil
    {"url": "https://news.google.com/rss/search?q=Coca-Cola+FEMSA+PepsiCo+Brasil+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Coca-Cola FEMSA | PepsiCo Brasil
    {"url": "https://news.google.com/rss/search?q=JBS+BRF+Seara+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # JBS | BRF | Seara
    {"url": "https://news.google.com/rss/search?q=Natura+Grupo+Boticário+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Natura | Grupo Boticário
    {"url": "https://news.google.com/rss/search?q=Unilever+Brasil+Colgate-Palmolive+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Unilever Brasil | Colgate-Palmolive
    {"url": "https://news.google.com/rss/search?q=L'Oréal+Brasil+Reckitt+Brasil+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # L'Oréal Brasil | Reckitt Brasil
    {"url": "https://news.google.com/rss/search?q=Hypera+Farmacêutica+Eurofarma+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Hypera Farmacêutica | Eurofarma
    {"url": "https://news.google.com/rss/search?q=Whirlpool+Electrolux+Brasil+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Whirlpool | Electrolux Brasil
    {"url": "https://news.google.com/rss/search?q=Samsung+Brasil+LG+Brasil+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Samsung Brasil | LG Brasil
    {"url": "https://news.google.com/rss/search?q=Foxconn+Brasil+Lenovo+Brasil+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Foxconn Brasil | Lenovo Brasil
    {"url": "https://news.google.com/rss/search?q=Mercedes-Benz+Brasil+Ford+Brasil+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Mercedes-Benz Brasil | Ford Brasil
    {"url": "https://news.google.com/rss/search?q=Volkswagen+Brasil+Renault+Brasil+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Volkswagen Brasil | Renault Brasil
    {"url": "https://news.google.com/rss/search?q=John+Deere+Brasil+AGCO+Brasil+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # John Deere Brasil | AGCO Brasil
    {"url": "https://news.google.com/rss/search?q=Embraer+Braskem+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Embraer | Braskem
    {"url": "https://news.google.com/rss/search?q=Suzano+Bridgestone+Brasil+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Suzano | Bridgestone Brasil
    {"url": "https://news.google.com/rss/search?q=Bosch+Brasil+Electrolux+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Bosch Brasil | Electrolux
    {"url": "https://news.google.com/rss/search?q=Petz+Petlove+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Petz | Petlove
    {"url": "https://news.google.com/rss/search?q=RD+Raia+Drogasil+Grupo+DPSP+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # RD Raia Drogasil | Grupo DPSP
    {"url": "https://news.google.com/rss/search?q=Viveo+Iron+Mountain+Brasil+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Viveo | Iron Mountain Brasil
    {"url": "https://news.google.com/rss/search?q=Ascenty+Scala+Data+Centers+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Ascenty | Scala Data Centers
    {"url": "https://news.google.com/rss/search?q=Infracommerce+Grupo+SBF+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Infracommerce | Grupo SBF
    {"url": "https://news.google.com/rss/search?q=Arezzo+Zara+Brasil+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Arezzo | Zara Brasil
    {"url": "https://news.google.com/rss/search?q=Decathlon+Brasil+Fast+Shop+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Decathlon Brasil | Fast Shop
    {"url": "https://news.google.com/rss/search?q=Tok+Stok+MadeiraMadeira+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Tok Stok | MadeiraMadeira
    {"url": "https://news.google.com/rss/search?q=Kalunga+Lojas+Colombo+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Kalunga | Lojas Colombo
    {"url": "https://news.google.com/rss/search?q=Patrus+Transportes+Braspress+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Patrus Transportes | Braspress
    {"url": "https://news.google.com/rss/search?q=Jamef+Expresso+3300+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Jamef | Expresso 3300
    {"url": "https://news.google.com/rss/search?q=Transportadora+Minuano+Plimor+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # Transportadora Minuano | Plimor
    {"url": "https://news.google.com/rss/search?q=J&T+Express+Brasil+Anjun+Brasil+galpão+OR+logística+OR+distribuição+OR+expansão+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},  # J&T Express Brasil | Anjun Brasil
]

# ─────────────────────────────────────────────
# PALAVRAS-CHAVE (gatilho de coleta)
# ─────────────────────────────────────────────

# ── Grupo 1: Âncoras logísticas — imóvel, infraestrutura ou operação física ──
KEYWORDS_ANCORA = [
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
    "operação logística", "operações logísticas",
    "cadeia logística", "cadeia de abastecimento", "supply chain",
    "3pl", "operador logístico", "operadores logísticos",
    "build to suit", "bts logístico", "sale and leaseback",
    "fundo logístico", "real estate logístico",
]

# ── Grupo 2: Sinais de ação — movimento corporativo ou financeiro ──
KEYWORDS_ACAO = [
    # inaugurar
    "inaugura", "inaugurou", "inauguraram", "inauguração", "inaugurações",
    "inaugurado", "inaugurada",
    # expandir
    "expande", "expandiu", "expandiram", "expansão", "expansões", "expandido",
    # ampliar
    "amplia", "ampliou", "ampliaram", "ampliação", "ampliado",
    # abrir
    "abre", "abriu", "abriram", "abertura", "abrirá",
    "nova unidade", "novas unidades", "nova operação", "novas operações",
    "nova fase", "novas instalações", "novo cd", "novo galpão", "novos galpões",
    # investir
    "investe", "investiu", "investiram", "investimento", "investimentos", "aporte",
    # contratar
        # instalar
    "instala", "instalou", "instalaram", "instalação",
    # construir
    "constrói", "construiu", "construção",
    # implantar
    "implanta", "implantou", "implantação",
    # crescer
    "cresce", "cresceu", "crescimento",
    # locar / alugar
    "locação", "aluguel", "aluga", "alugou",
    # emissão / captação financeira
    "cri logístico", "captação logística",
]

# ── Grupo 3: Empresas monitoradas — presença garante relevância ──
KEYWORDS_EMPRESA = [
    "mercado livre", "mercadolivre",
    "shopee", "amazon brasil", "amazon logística",
    "magazine luiza", "magalu",
    "via varejo", "casas bahia", "ponto frio",
    "b2w", "americanas",
    "shein", "dafiti", "netshoes",
    "madeira madeira", "fast shop", "tok stok", "tok&stok",
    "lojas renner", "renner", "riachuelo", "c&a",
    "pernambucanas", "grupo mateus", "kalunga", "lojas colombo",
    "lojas lebes", "lojas leader", "centauro", "arezzo",
    "zara brasil", "decathlon brasil", "westwing", "mobly",
    "grupo muffato", "atacadão", "assaí", "grupo zaffari",
    "dhl", "fedex", "ups brasil", "correios", "jadlog",
    "total express", "loggi", "azul cargo", "jamef", "braspress",
    "patrus transportes", "plimor", "expresso 3300",
    "transportadora minuano", "sigma transportes", "unilog express",
    "j&t express", "anjun brasil", "movvi logística",
    "luft logistics", "id logistics", "ceva logistics",
    "kuehne nagel", "kuehne+nagel", "dsv", "fm logistic",
    "martin brower", "maersk", "yusen logistics",
    "andreani", "bomi group", "celistics", "multilog",
    "grupo tecadi", "comfrio", "friozem", "superfrio",
    "ambev", "coca-cola femsa", "heineken", "pepsico",
    "bauducco", "brf", "seara", "jbs", "natura",
    "grupo boticário", "boticário", "l'oréal", "loreal",
    "unilever", "colgate", "sanofi", "reckitt", "hypera",
    "eurofarma", "whirlpool", "electrolux", "midea",
    "samsung brasil", "lenovo brasil", "foxconn", "semp tcl",
    "bosch", "mercedes benz", "mercedes-benz", "ford brasil",
    "gm brasil", "general motors", "renault brasil",
    "volkswagen brasil", "agco brasil", "john deere", "embraer",
    "braskem", "suzano", "bridgestone", "benteler",
    "ascenty", "scala data centers", "iron mountain",
    "petz", "petlove", "rd raia drogasil", "grupo dpsp",
    "viveo", "tpc logística", "grupo martins",
    "infracommerce", "grupo sbf",
]

# Mantém KEYWORDS como union dos três grupos para compatibilidade
KEYWORDS = list(set(KEYWORDS_ANCORA + KEYWORDS_ACAO + KEYWORDS_EMPRESA))

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
    """Formata data relativa em horário de Brasília (BRT = UTC-3)."""
    agora   = datetime.now(timezone.utc)
    dt_brt  = dt.astimezone(BRT)
    diff    = agora - dt
    minutos = int(diff.total_seconds() / 60)
    if minutos < 60:
        return f"há {minutos} min"
    horas = int(diff.total_seconds() / 3600)
    if horas < 24:
        return f"hoje, {dt_brt.strftime('%H:%M')}"
    if horas < 48:
        return f"ontem, {dt_brt.strftime('%H:%M')}"
    return dt_brt.strftime("%d/%m às %H:%M")


def _texto_completo(entry) -> str:
    titulo  = getattr(entry, "title", "") or ""
    summary = getattr(entry, "summary", "") or ""
    # Remove tags HTML simples do summary
    summary = re.sub(r"<[^>]+>", " ", summary)
    return f"{titulo}. {summary}".lower()


def _contar_keywords(texto: str) -> int:
    return sum(1 for kw in KEYWORDS if kw in texto)

def _tem_keyword(texto: str) -> bool:
    """Filtragem em dois grupos:
    - Notícias com empresa monitorada + ancora OU empresa + ação → relevantes
    - Notícias sem empresa precisam de ancora + ação (contexto logístico confirmado)
    - Evita capturar notícias de outros setores que usam palavras genéricas
    """
    tem_ancora  = any(kw in texto for kw in KEYWORDS_ANCORA)
    tem_acao    = any(kw in texto for kw in KEYWORDS_ACAO)
    tem_empresa = any(kw in texto for kw in KEYWORDS_EMPRESA)

    if tem_empresa:
        # Empresa monitorada + ancora OU ação já é suficiente
        return tem_ancora or tem_acao
    else:
        # Sem empresa conhecida: exige ancora logística + ação
        return tem_ancora and tem_acao


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




def _extrair_fonte(link: str, fonte_feed: str) -> str:
    """Extrai o nome do portal a partir do link da notícia.
    Se o link for do Google News ou inválido, usa o nome do feed como fallback.
    """
    from urllib.parse import urlparse

    NOMES_PORTAIS = {
        "valor.globo.com":          "Valor Econômico",
        "exame.com":                "Exame",
        "infomoney.com.br":         "InfoMoney",
        "cnnbrasil.com.br":         "CNN Brasil",
        "logisticadescomplicada.com": "Logística Descomplicada",
        "portosenavios.com.br":     "Portos e Navios",
        "logisticsnews.com.br":     "Logistics News",
        "fundsexplorer.com.br":     "Funds Explorer",
        "buildings.com.br":         "Buildings",
        "griclub.org":              "GRI Club",
        "ecommercebrasil.com.br":   "E-commerce Brasil",
        "automotivebusiness.com.br":"Automotive Business",
        "mundologistica.com.br":    "Mundo Logística",
        "modaisemfoco.com.br":      "Modais em Foco",
        "tecnologistica.com.br":    "Tecnologística",
        "investnews.com.br":        "InvestNews",
        "moneyreport.com.br":       "Money Report",
        "moneytimes.com.br":        "Money Times",
        "terra.com.br":             "Terra",
        "uol.com.br":               "UOL",
        "folha.uol.com.br":         "Folha de S.Paulo",
        "estadao.com.br":           "Estadão",
        "g1.globo.com":             "G1",
        "oglobo.globo.com":         "O Globo",
        "gazetadopovo.com.br":      "Gazeta do Povo",
        "correio24horas.com.br":    "Correio 24h",
        "correiobraziliense.com.br":"Correio Braziliense",
        "ndmais.com.br":            "ND Mais",
        "gauchazh.cne.com.br":      "GaúchaZH",
        "nsctotal.com.br":          "NSC Total",
        "opovo.com.br":             "O Povo",
        "diariodopernambuco.com.br":"Diário de Pernambuco",
        "jornaldocomercio.com.br":  "Jornal do Comércio",
        "agazeta.com.br":           "A Gazeta",
        "segs.com.br":              "SEGS",
        "suno.com.br":              "Suno",
        "bloomberg.com.br":         "Bloomberg",
        "reuters.com":              "Reuters",
        "tiinside.com.br":          "TI Inside",
        "sobral.news":              "Sobral Online",
        "gcmais.com.br":            "GC Mais",
    }

    if not link or link == "#" or "news.google.com" in link:
        return fonte_feed

    try:
        domain = urlparse(link).netloc.lower().replace("www.", "")
        # Verifica match exato primeiro
        if domain in NOMES_PORTAIS:
            return NOMES_PORTAIS[domain]
        # Verifica match parcial (subdomínios)
        for key, nome in NOMES_PORTAIS.items():
            if key in domain:
                return nome
        # Fallback: capitaliza o domínio sem TLD
        nome_raw = domain.split(".")[0].replace("-", " ").title()
        return nome_raw if len(nome_raw) > 2 else fonte_feed
    except Exception:
        return fonte_feed


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
    log.info(f"Janela de busca: ultimas {horas}h (corte BRT: {corte.astimezone(BRT).strftime('%Y-%m-%d %H:%M')})")
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

            # Salva as keywords encontradas para exibição no portal
            kws_ancora  = [kw for kw in KEYWORDS_ANCORA  if kw in texto]
            kws_acao    = [kw for kw in KEYWORDS_ACAO    if kw in texto]
            kws_empresa = [kw for kw in KEYWORDS_EMPRESA if kw in texto]
            kws_matches = list(dict.fromkeys(kws_empresa + kws_ancora + kws_acao))[:6]

            candidatas.append({
                "id":       uid,
                "headline": titulo,
                "source":   _extrair_fonte(link_real, fonte),
                "link":     link_real,
                "time":     _formata_tempo(dt),
                "dt_iso":   dt.isoformat(),
                "summary":  re.sub(r"<[^>]+>", " ", getattr(entry, "summary", "") or "").strip()[:600],
                "category": _categoria(texto),
                "keywords": kws_matches,
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
    tem_ancora  = any(kw in texto for kw in KEYWORDS_ANCORA)
    tem_empresa = any(kw in texto for kw in KEYWORDS_EMPRESA)
    # Score: base 5, +1 por keyword, bônus por ancora+empresa
    score = min(9, 4 + kw_count + (1 if tem_ancora and tem_empresa else 0))
    # Destaque: ancora + empresa + 3+ keywords
    if tem_ancora and tem_empresa and kw_count >= 3:
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
        "gerado_em": datetime.now(BRT).strftime("%d/%m/%Y às %H:%M"),
        "total": len(filtradas),
        "noticias": filtradas,
    }
    OUTPUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(f"JSON salvo em: {OUTPUT_JSON} ({len(filtradas)} notícias)")



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

    # 5. Salvar JSON
    gerar_json(top)
    log.info("Coleta concluída com sucesso.")


if __name__ == "__main__":
    main()