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

Patch v1.6.1: alertas com tipo == "saida" sao tratados ANTES da validacao de
entrada (nao trazem score, timing_confirmado, taxa_acerto, expectancia).

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


# --------------------------- VALIDACAO DO PAYLOAD (ENTRADA) ---------------------------
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


# --------------------------- VALIDACAO E MENSAGEM (SAIDA) - Patch v1.6.1 ---------------------------
CAMPOS_SAIDA = ["ativo", "direcao", "motivo", "entrada", "stop"]


def _validar_saida(payload: dict):
    if WEBHOOK_SHARED_SECRET and payload.get("secret") != WEBHOOK_SHARED_SECRET:
        raise HTTPException(status_code=401, detail="Segredo do webhook invalido")
    faltando = [c for c in CAMPOS_SAIDA if c not in payload]
    if faltando:
        raise HTTPException(status_code=422, detail=f"Campos faltando no payload de saida: {faltando}")


def _montar_mensagem_saida(payload: dict) -> str:
    ativo = payload["ativo"]
    direcao = "COMPRA" if str(payload["direcao"]).lower().startswith("c") else "VENDA"
    timeframe = payload.get("timeframe")
    cab = f"SAIDA {direcao} — {ativo} ({timeframe})" if timeframe else f"SAIDA {direcao} — {ativo}"
    linhas = [
        cab,
        f"Motivo: {payload['motivo']}",
        f"Entrada: {payload['entrada']} | Stop: {payload['stop']}",
    ]
    if payload.get("alvo1") is not None and payload.get("alvo2") is not None:
        linhas.append(f"Alvo 1: {payload['alvo1']} | Alvo 2: {payload['alvo2']}")
    pm = payload.get("preco_medio")
    if pm:
        linhas.append(f"Preço médio (referência): {pm}")
    kelly = payload.get("kelly_pct")
    if kelly is not None:
        linhas.append(f"Kelly do plano: {float(kelly):.2f}% do capital")
    linhas.append("Ação sugerida: avaliar encerrar o restante da posição (execução manual, sempre com você).")
    return "\n".join(linhas)


def _liberar_risco_do_ativo(ativo: str):
    # Saida do plano libera o risco agregado daquele ativo.
    with _lock:
        restantes = [s for s in _janela_sinais if s["ativo"] != ativo]
        _janela_sinais.clear()
        _janela_sinais.extend(restantes)


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
    # Refino P7 - todos os campos abaixo sao OPCIONAIS no payload (via .get()), para nao
    # quebrar alertas antigos (v1 ou v1.2 anteriores a Parte 7) que ainda nao mandam esses campos.
    timeframe = payload.get("timeframe")

    cabecalho = f"{ativo} ({timeframe}) — {direcao}" if timeframe else f"{ativo} — {direcao}"
    linhas = [
        cabecalho,
        # Score: na v1.2 o score e continuo e pode passar de 4 com os bonus todos ligados -
        # rotulo sem teto para nao sugerir um limite que nao existe mais.
        f"Score: {score:.2f} | Timing (Velez): {timing}",
        f"Amostra: {amostra_atual}/{amostra_exigida} | Taxa histórica: {taxa:.0%} | Expectância: {expect:.2f}R",
        f"Tamanho sugerido (Kelly ajustado): {kelly:.2f}% do capital",
    ]

    # Refino P7 - plano de trade (Entrada/Stop/Alvo1/Alvo2 com probabilidade) e sugestao de
    # saida parcial, a partir do payload novo do Pine (rompimento/lado oposto do candle do
    # sinal - mecanica convergente de Velez e de um post real do Heeger/Didi sobre XAUUSD).
    entrada = payload.get("entrada")
    stop = payload.get("stop")
    alvo1 = payload.get("alvo1")
    alvo2 = payload.get("alvo2")
    if entrada is not None and stop is not None:
        linhas.append(f"Entrada (rompimento do candle do sinal): {entrada} | Stop (lado oposto do mesmo candle): {stop}")
    if alvo1 is not None and alvo2 is not None:
        amostra_alvos_suf = payload.get("amostra_alvos_suficiente", False)
        prob_alvo1 = payload.get("prob_alvo1")
        prob_alvo2 = payload.get("prob_alvo2")
        prob1_txt = f"{prob_alvo1:.0%}" if amostra_alvos_suf and prob_alvo1 is not None else "amostra insuficiente"
        prob2_txt = f"{prob_alvo2:.0%}" if amostra_alvos_suf and prob_alvo2 is not None else "amostra insuficiente"
        linhas.append(f"Alvo 1: {alvo1} (prob. histórica: {prob1_txt}) | Alvo 2: {alvo2} (prob. histórica: {prob2_txt})")
        pct1 = payload.get("pct_parcial1")
        pct2 = payload.get("pct_parcial2")
        if pct1 is not None and pct2 is not None:
            linhas.append(
                f"Sugestão de saída parcial: {pct1:.0f}% no Alvo 1, {pct2:.0f}% no Alvo 2, restante em trailing "
                f"(sugestão própria editável — NÃO é regra documentada de Velez/Didi, só o CONCEITO de "
                f"'sair em partes' é real)."
            )

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

    # Alerta de saida (Pine v1.6.1) - nao passa pela validacao de entrada
    if str(payload.get("tipo", "sinal")).lower() == "saida":
        _validar_saida(payload)
        mensagem = _montar_mensagem_saida(payload)
        _enviar_telegram(mensagem)
        _registrar_no_notion(payload, mensagem)
        _liberar_risco_do_ativo(payload["ativo"])
        return {"status": "ok", "tipo": "saida"}

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
