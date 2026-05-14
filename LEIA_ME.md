# EREA — Portal de Notícias | Time de Locações
## Guia atualizado do sistema

---

## Estrutura do repositório

```
erea-portal/
├── portal_noticias_erea.html   ← portal web (favorito do time)
├── coletor.py                  ← script de coleta (roda automaticamente)
├── news_data.json              ← gerado pelo coletor a cada execução
├── .github/
│   └── workflows/
│       └── coletar.yml         ← agendamento automático via GitHub Actions
└── LEIA_ME.md                  ← este arquivo
```

---

## Como funciona

O sistema tem duas partes:

**1. Coletor (`coletor.py`)** — roda automaticamente todo dia às **06:00 BRT** via GitHub Actions, sem necessidade de computador ligado. Ele:
- Percorre **97 feeds RSS** de portais especializados e Google News
- Busca notícias das **últimas 7 dias** com filtro de palavras-chave logísticas
- Monitora **200+ empresas** com feeds dedicados por empresa
- Aplica deduplicação por similaridade de título
- Salva as **50 notícias mais relevantes** em `news_data.json`

**2. Portal (`portal_noticias_erea.html`)** — página web hospedada no GitHub Pages. Ao abrir, lê o `news_data.json` e exibe as notícias com filtros, badges e botão de busca no Google.

**URL do portal:**
```
https://erea-advisory.github.io/erea-portal/portal_noticias_erea.html
```

---

## Configurações principais (`coletor.py`)

| Variável | Valor atual | O que faz |
|---|---|---|
| `HORAS_JANELA` | `168` | Janela de busca (7 dias) |
| `MAX_NOTICIAS` | `50` | Máximo de notícias salvas |
| `PAUSA_API` | `0.5` | Pausa entre chamadas (segundos) |
| `FEED_TIMEOUT` | `10` | Timeout por feed RSS (segundos) |

---

## Agendamento automático

O arquivo `.github/workflows/coletar.yml` contém:
```yaml
schedule:
  - cron: '0 9 * * *'   # 06:00 BRT (UTC-3) — todos os dias
```

**Se o agendamento parar de funcionar** (pode acontecer após inatividade ou transferência de repositório):
1. Acesse o repositório no GitHub
2. Edite qualquer arquivo (ex: adicione um espaço neste `LEIA_ME.md`)
3. Faça commit — isso "acorda" o agendador
4. O workflow voltará a rodar automaticamente no próximo horário

Para rodar manualmente:
- Acesse **Actions → EREA — Coletor de Notícias → Run workflow**

---

## Palavras-chave

O coletor usa três grupos de keywords:

**Âncoras logísticas** — imóvel ou infraestrutura física
`galpão`, `armazém`, `centro de distribuição`, `hub logístico`, `fulfillment`, `cross-docking`, `last mile`, `build to suit`, `operador logístico`, `supply chain`...

**Sinais de ação** — movimento corporativo
`inaugura`, `inaugurou`, `expande`, `expandiu`, `amplia`, `ampliou`, `abre`, `abriu`, `investe`, `investiu`, `assina contrato`, `entra em operação`, `planeja expansão`, `anuncia expansão`...

**Empresas monitoradas** — 200+ empresas com variações de grafia
Mercado Livre, Shopee, Amazon, DHL, Jadlog, Total Express, Loggi, Ambev, JBS, Natura, Boticário, Unilever, Samsung, Volkswagen, Embraer, Petz...

**Regra de filtragem:**
- Notícia com **empresa monitorada** → precisa de ancora OU ação
- Notícia **sem empresa** → precisa de ancora E ação

---

## Adicionar nova empresa ou keyword

Abra `coletor.py` e edite:
- `KEYWORDS_ANCORA` — âncoras logísticas
- `KEYWORDS_ACAO` — sinais de ação
- `KEYWORDS_EMPRESA` — empresas monitoradas
- `FEEDS` — feeds RSS e Google News

Para adicionar feed dedicado de nova empresa:
```python
{"url": "https://news.google.com/rss/search?q=Nome+Empresa+logística+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419", "fonte": "Google News"},
```

---

## Análise de relevância

Atualmente gerada por **regras automáticas** baseadas nas keywords encontradas. O score (1–9) é proporcional ao número de matches.

A integração com a **API Claude** está preparada no código. Quando ativada, cada notícia recebe análise específica para o time de locações. Para ativar:
1. Acesse **console.anthropic.com** e obtenha uma chave `sk-ant-...`
2. No GitHub: **Settings → Secrets and variables → Actions → New repository secret**
3. Nome: `ANTHROPIC_API_KEY` | Valor: sua chave

---

## Dúvidas

Contato interno: time de Research EREA.
