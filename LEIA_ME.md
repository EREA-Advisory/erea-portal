# EREA — Portal de Notícias | Time de Locações
## Guia de instalação e uso

---

## Estrutura de arquivos

```
pasta-do-portal/
├── portal_noticias_erea.html   ← salve como favorito no browser
├── coletor.py                  ← script Python (roda às 7h)
├── news_data.json              ← gerado automaticamente pelo coletor
├── coletor.log                 ← log de execuções
└── LEIA_ME.md                  ← este arquivo
```

> **Importante:** todos os arquivos devem ficar na **mesma pasta**.

---

## 1. Instalar dependências Python

Abra o terminal (cmd ou PowerShell no Windows) e execute:

```bash
pip install feedparser requests
```

---

## 2. Configurar a chave da API Anthropic

O coletor usa Claude para pontuar e escrever a análise de relevância de cada notícia.

**Opção A — variável de ambiente (recomendado):**

```bash
# Windows (PowerShell)
$env:ANTHROPIC_API_KEY = "sk-ant-..."

# Mac / Linux
export ANTHROPIC_API_KEY="sk-ant-..."
```

**Opção B — direto no arquivo:**

Abra `coletor.py` e substitua na linha 22:
```python
ANTHROPIC_API_KEY = "sk-ant-SUA_CHAVE_AQUI"
```

> Sem a chave, o coletor ainda funciona — apenas sem a análise de relevância gerada por IA.

---

## 3. Testar manualmente

```bash
python coletor.py
```

Após ~2 minutos (dependendo da quantidade de feeds), o arquivo `news_data.json` será criado.
Abra o `portal_noticias_erea.html` no browser para ver as notícias reais.

---

## 4. Agendar execução diária às 7h

### Windows — Agendador de Tarefas

1. Abra **Agendador de Tarefas** → "Criar Tarefa Básica"
2. Nome: `EREA Coletor de Notícias`
3. Gatilho: **Diário** → 07:00 → repetir dias da semana (marque "Segunda a Sexta")
4. Ação: **Iniciar um programa**
   - Programa: `python` (ou o caminho completo, ex: `C:\Python312\python.exe`)
   - Argumentos: `C:\caminho\para\coletor.py`
5. Marque **"Executar mesmo que o usuário não esteja conectado"**

### Mac / Linux — cron

```bash
crontab -e
```

Adicione a linha:
```
0 7 * * 1-5 /usr/bin/python3 /caminho/completo/coletor.py >> /caminho/completo/coletor.log 2>&1
```

---

## 5. Salvar o portal como favorito

1. Abra `portal_noticias_erea.html` no Chrome ou Edge
2. Na barra de endereço, o caminho será algo como `file:///C:/portal/portal_noticias_erea.html`
3. Pressione `Ctrl+D` (ou `Cmd+D` no Mac) → salve como **"Portal EREA Locações"**
4. Adicione à barra de favoritos para acesso rápido

> O portal carrega o `news_data.json` automaticamente ao abrir.
> Se o arquivo ainda não existir, exibe os dados de exemplo até a primeira execução do coletor.

---

## 6. Personalizar palavras-chave

Abra `coletor.py` e edite a lista `KEYWORDS` (linha ~70).  
Para adicionar um novo feed RSS, adicione um item à lista `FEEDS` (linha ~45).

---

## 7. Configurações principais

| Variável no coletor.py | Padrão | O que faz |
|---|---|---|
| `HORAS_JANELA` | `24` | Busca notícias das últimas N horas |
| `MAX_NOTICIAS` | `20` | Máximo de notícias salvas no JSON |
| `PAUSA_API` | `1.2` | Pausa entre chamadas Claude (segundos) |

---

## Dúvidas?

Contato interno: time de Research EREA.
