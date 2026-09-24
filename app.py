"""
Servidor de Orquestracao - Motor Multiativo de Confluencia
============================================================
Recebe o webhook bruto do TradingView (payload montado pelo alert() do Pine
Script), aplica a etapa de filtro que so faz sentido fora do grafico (teto de
risco agregado entre sinais simultaneos de ativos diferentes), monta a
mensagem final (numeros antes da narrativa) e entrega no Telegram. Registra
cada sinal no Notion como "sugerido / nao operado" ate voce confirmar.

Este servidor NAO decide direcao nem tamanho de posicao - isso ja vem pronto
no payload, calculado dentro do Pine Script (score de blocos, timing Velez,
taxa historica, expectancia, Kelly). O servidor so orquestra a ENTREGA:
  1) cascata final (teto de risco agregado da cesta)
  2) ranking por expectancia se varios sinais chegarem juntos
  3) narrativa (texto humano gerado a partir dos numeros)
  4) entrega no Telegram
  5) registro no Notion
A execucao da ordem continua 100% manual, sempre com voce.

Deploy sugerido: Render/Railway (free tier) ou uma VPS pequena. Nao rode isso
no celular - precisa de processo continuo em background, o que apps moveis
nao sustentam de forma confiavel.
"""

import os
import time
import threading
import requests
from collections import deque
from fastapi import FastAPI, Request, HTTPException

# --------------------------- CONFIGURACAO (variaveis de ambiente) ---------------------------
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]          # ex.: 8811625956:AAH82xCH-...
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]              # ex.: 824539270
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")                  # integracao interna do Notion (opcional na v1)
NOTION_LOG_PAGE_OR_DB_ID = os.environ.get("NOTION_LOG_PAGE_OR_DB_ID")  # onde registrar os sinais
WEBHOOK_SHARED_SECRET = os.environ.get("WEBHOOK_SHARED_SECRET", "")    # token simples no payload, evita spoof
TETO_RISCO_AGREGADO_PCT = float(os.environ.get("TETO_RISCO_AGREGADO_PCT", "6.0"))
JANELA_AGRUPAMENTO_SEG = float(os.environ.get("JANELA_AGRUPAMENTO_SEG", "8"))  # agrupa sinais quase simultaneos

app = FastAPI(title="Motor Multiativo - Orquestrador")

# Estado em memoria dos sinais entregues na sessao/candle atual, para o teto de risco agregado.
# Em producao real, trocar por um armazenamento persistente (ex.: SQLite) se o servidor reiniciar.
_lock = threading.Lock()
_janela_sinais = deque()  # cada item: {"ativo":..., "kelly_pct":..., "timestamp":...}


def _limpar_janela_expirada():
    agora = time.time()
    while _janela_sinais and agora - _janela_sinais[0]["timestamp"] > 60 * 30:
        # limpa sinais com mais de 30 min - janela de "exposicao simultanea" considerada relevante
        _janela_sinais.popleft()


def _risco_agregado_atual() -> float:
    _limpar_janela_expirada()
    return sum(s["kelly_pct"] for s in _janela_sinais)


# --------------------------- VALIDACAO DO PAYLOAD ---------------------------
CAMPOS_OBRIGATORIOS = [
    "ativo", "direcao", "score", "timing_confirmado",
    "amostra_atual", "amostra_exigida", "taxa_acerto", "expectancia", "kelly_pct",
]


def _validar_payload(payload: dict):
    faltando = [c for c in CAMPOS_OBRIGATORIOS if c not in payload]
    if faltando:
        raise HTTPException(status_code=422, detail=f"Campos faltando no payload: {faltando}")
    if WEBHOOK_SHARED_SECRET and payload.get("secret") != WEBHOOK_SHARED_SECRET:
        raise HTTPException(status_code=401, detail="Segredo do webhook invalido")


# --------------------------- NARRATIVA (numeros antes da frase, sempre) ---------------------------
def _montar_mensagem(payload: dict, dentro_do_teto: bool, risco_pos_sinal: float) -> str:
    ativo = payload["ativo"]
    direcao = "COMPRA" if payload["direcao"].lower().startswith("c") or payload["direcao"].lower() == "buy" else "VENDA"
    score = payload["score"]
    timing = "confirmado" if payload["timing_confirmado"] else "pendente"
    amostra_atual = payload["amostra_atual"]
    amostra_exigida = payload["amostra_exigida"]
    taxa = payload["taxa_acerto"]
    expect = payload["expectancia"]
    kelly = payload["kelly_pct"]
    aviso_classe = payload.get("aviso_classe")  # ex.: "sem leitura fundamentalista/sazonal (softs)"

    linhas = [
        f"{ativo} — {direcao}",
        f"Score: {score}/4 | Timing (Velez): {timing}",
        f"Amostra: {amostra_atual}/{amostra_exigida} | Taxa histórica: {taxa:.0%} | Expectância: {expect:.2f}R",
        f"Tamanho sugerido (Kelly ajustado): {kelly:.2f}% do capital",
    ]
    if not dentro_do_teto:
        linhas.append(
            f"⚠️ Fora do teto de risco agregado ({risco_pos_sinal:.1f}% > {TETO_RISCO_AGREGADO_PCT:.1f}%) "
            f"— sinal informativo, não somar posição nova agora."
        )
    if aviso_classe:
        linhas.append(f"Aviso: {aviso_classe}")
    if amostra_atual < amostra_exigida:
        linhas.append("Aviso: amostra ainda insuficiente — tratar como observação, não como sinal validado.")

    return "\n".join(linhas)


# --------------------------- ENTREGA ---------------------------
def _enviar_telegram(texto: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": texto}, timeout=10)
    resp.raise_for_status()


def _registrar_no_notion(payload: dict, mensagem: str):
    if not (NOTION_TOKEN and NOTION_LOG_PAGE_OR_DB_ID):
        return  # registro no Notion e opcional na v1 - requer integracao interna configurada por voce
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    titulo_texto = f"{payload['ativo']} - {payload['direcao']} - sugerido"
    bloco_titulo = {"text": {"content": titulo_texto}}
    bloco_corpo = {"text": {"content": mensagem}}
    corpo = {
        "parent": {"page_id": NOTION_LOG_PAGE_OR_DB_ID},
        "properties": {"title": [bloco_titulo]},
        "children": [
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [bloco_corpo]},
            }
        ],
    }
    requests.post("https://api.notion.com/v1/pages", headers=headers, json=corpo, timeout=10)


# --------------------------- ENDPOINT PRINCIPAL ---------------------------
@app.post("/webhook/tradingview")
async def webhook_tradingview(request: Request):
    payload = await request.json()
    _validar_payload(payload)

    with _lock:
        _limpar_janela_expirada()
        risco_antes = _risco_agregado_atual()
        risco_depois = risco_antes + float(payload["kelly_pct"])
        dentro_do_teto = risco_depois <= TETO_RISCO_AGREGADO_PCT
        if dentro_do_teto:
            _janela_sinais.append({
                "ativo": payload["ativo"],
                "kelly_pct": float(payload["kelly_pct"]),
                "timestamp": time.time(),
            })

    mensagem = _montar_mensagem(payload, dentro_do_teto, risco_depois)
    _enviar_telegram(mensagem)
    _registrar_no_notion(payload, mensagem)

    return {"status": "ok", "dentro_do_teto": dentro_do_teto, "risco_agregado_pct": round(risco_depois, 2)}


@app.get("/health")
async def health():
    return {"status": "ok"}
