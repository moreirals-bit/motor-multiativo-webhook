"""
Servidor de Orquestracao - Motor Multiativo de Confluencia + Oliver v0.3
========================================================================
Recebe webhooks do TradingView (alert() do Pine Script) e entrega no Telegram.

Endpoints:
  POST /webhook/tradingview  -> Motor Multiativo (sinal + saida)
  POST /webhook/oliver       -> Oliver Velez v0.3 (entrada, parcial, trailing, saida, conflito)
  GET  /health

A execucao da ordem continua 100% manual, sempre com voce.
"""

import os
import json
import time
import logging
import threading
import requests
from collections import deque
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("motor")
log_oliver = logging.getLogger("oliver")

# --------------------------- CONFIGURACAO (variaveis de ambiente) ---------------------------
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")                          # opcional
NOTION_LOG_PAGE_OR_DB_ID = os.environ.get("NOTION_LOG_PAGE_OR_DB_ID")  # opcional
WEBHOOK_SHARED_SECRET = os.environ.get("WEBHOOK_SHARED_SECRET", "")
TETO_RISCO_AGREGADO_PCT = float(os.environ.get("TETO_RISCO_AGREGADO_PCT", "6.0"))
JANELA_AGRUPAMENTO_SEG = float(os.environ.get("JANELA_AGRUPAMENTO_SEG", "8"))

app = FastAPI(title="Motor Multiativo - Orquestrador")

# Estado em memoria dos sinais entregues, para o teto de risco agregado.
_lock = threading.Lock()
_janela_sinais = deque()  # cada item: {"ativo":..., "kelly_pct":..., "timestamp":...}


def _limpar_janela_expirada():
    agora = time.time()
    while _janela_sinais and agora - _janela_sinais[0]["timestamp"] > 60 * 30:
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


CAMPOS_SAIDA = ["ativo", "direcao", "motivo", "entrada", "stop"]


def _validar_saida(payload: dict):
    if WEBHOOK_SHARED_SECRET and payload.get("secret") != WEBHOOK_SHARED_SECRET:
        raise HTTPException(status_code=401, detail="Segredo do webhook invalido")
    faltando = [c for c in CAMPOS_SAIDA if c not in payload]
    if faltando:
        raise HTTPException(status_code=422, detail=f"Campos faltando no payload de saida: {faltando}")


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
    aviso_classe = payload.get("aviso_classe")
    timeframe = payload.get("timeframe")

    cabecalho = f"{ativo} ({timeframe}) \u2014 {direcao}" if timeframe else f"{ativo} \u2014 {direcao}"
    linhas = [
        cabecalho,
        f"Score: {score:.2f} | Timing (Velez): {timing}",
        f"Amostra: {amostra_atual}/{amostra_exigida} | Taxa hist\u00f3rica: {taxa:.0%} | Expect\u00e2ncia: {expect:.2f}R",
        f"Tamanho sugerido (Kelly ajustado): {kelly:.2f}% do capital",
    ]

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
        linhas.append(f"Alvo 1: {alvo1} (prob. hist\u00f3rica: {prob1_txt}) | Alvo 2: {alvo2} (prob. hist\u00f3rica: {prob2_txt})")
        pct1 = payload.get("pct_parcial1")
        pct2 = payload.get("pct_parcial2")
        if pct1 is not None and pct2 is not None:
            linhas.append(
                f"Sugest\u00e3o de sa\u00edda parcial: {pct1:.0f}% no Alvo 1, {pct2:.0f}% no Alvo 2, restante em trailing "
                f"(sugest\u00e3o pr\u00f3pria edit\u00e1vel \u2014 N\u00c3O \u00e9 regra documentada de Velez/Didi, s\u00f3 o CONCEITO de "
                f"'sair em partes' \u00e9 real)."
            )

    if not dentro_do_teto:
        linhas.append(
            f"\u26a0\ufe0f Fora do teto de risco agregado ({risco_pos_sinal:.1f}% > {TETO_RISCO_AGREGADO_PCT:.1f}%) "
            f"\u2014 sinal informativo, n\u00e3o somar posi\u00e7\u00e3o nova agora."
        )
    if aviso_classe:
        linhas.append(f"Aviso: {aviso_classe}")
    if amostra_atual < amostra_exigida:
        linhas.append("Aviso: amostra ainda insuficiente \u2014 tratar como observa\u00e7\u00e3o, n\u00e3o como sinal validado.")

    return "\n".join(linhas)


def _montar_mensagem_saida(payload: dict) -> str:
    ativo = payload["ativo"]
    direcao = "COMPRA" if str(payload["direcao"]).lower().startswith("c") else "VENDA"
    timeframe = payload.get("timeframe")
    cab = f"SAIDA {direcao} \u2014 {ativo} ({timeframe})" if timeframe else f"SAIDA {direcao} \u2014 {ativo}"
    linhas = [
        cab,
        f"Motivo: {payload['motivo']}",
        f"Entrada: {payload['entrada']} | Stop: {payload['stop']}",
    ]
    if payload.get("alvo1") is not None and payload.get("alvo2") is not None:
        linhas.append(f"Alvo 1: {payload['alvo1']} | Alvo 2: {payload['alvo2']}")
    pm = payload.get("preco_medio")
    if pm:
        linhas.append(f"Pre\u00e7o m\u00e9dio (refer\u00eancia): {pm}")
    kelly = payload.get("kelly_pct")
    if kelly is not None:
        linhas.append(f"Kelly do plano: {float(kelly):.2f}% do capital")
    linhas.append("A\u00e7\u00e3o sugerida: avaliar encerrar o restante da posi\u00e7\u00e3o (execu\u00e7\u00e3o manual, sempre com voc\u00ea).")
    return "\n".join(linhas)


def _liberar_risco_do_ativo(ativo: str):
    # Saida do plano libera o risco agregado daquele ativo.
    with _lock:
        restantes = [s for s in _janela_sinais if s["ativo"] != ativo]
        _janela_sinais.clear()
        _janela_sinais.extend(restantes)


# --------------------------- ENTREGA ---------------------------
def _enviar_telegram(texto: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": texto}, timeout=10)
    resp.raise_for_status()


def _registrar_no_notion(payload: dict, mensagem: str):
    if not (NOTION_TOKEN and NOTION_LOG_PAGE_OR_DB_ID):
        return  # opcional: requer integracao interna configurada por voce
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    titulo_texto = f"{payload['ativo']} - {payload['direcao']} - sugerido"
    corpo = {
        "parent": {"page_id": NOTION_LOG_PAGE_OR_DB_ID},
        "properties": {"title": [{"text": {"content": titulo_texto}}]},
        "children": [
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"text": {"content": mensagem}}]},
            }
        ],
    }
    requests.post("https://api.notion.com/v1/pages", headers=headers, json=corpo, timeout=10)


def _entregar(mensagem: str, payload: dict):
    try:
        _enviar_telegram(mensagem)
    except Exception as e:
        log.exception("Falha ao enviar Telegram: %s", e)
    try:
        _registrar_no_notion(payload, mensagem)
    except Exception as e:
        log.exception("Falha ao registrar no Notion: %s", e)


# --------------------------- ENDPOINT PRINCIPAL (Motor Multiativo) ---------------------------
@app.post("/webhook/tradingview")
async def webhook_tradingview(request: Request, background: BackgroundTasks):
    corpo = await request.body()
    log.info("Webhook recebido (%d bytes): %s", len(corpo), corpo[:300])
    try:
        payload = json.loads(corpo)
    except Exception:
        log.error("JSON invalido recebido: %s", corpo[:500])
        raise HTTPException(status_code=400, detail="JSON invalido")

    if str(payload.get("tipo", "sinal")).lower() == "saida":
        _validar_saida(payload)
        mensagem = _montar_mensagem_saida(payload)
        _liberar_risco_do_ativo(payload["ativo"])
        background.add_task(_entregar, mensagem, payload)
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
    background.add_task(_entregar, mensagem, payload)  # responde 200 na hora; entrega em segundo plano
    return {"status": "ok", "dentro_do_teto": dentro_do_teto, "risco_agregado_pct": round(risco_depois, 2)}


@app.get("/health")
async def health():
    return {"status": "ok"}


# ======================= OLIVER v0.3 \u2014 endpoint /webhook/oliver =======================
# Independente do Motor Multiativo: sem score, Kelly, amostra do motor ou teto de risco.
# Apenas formata os eventos que o Pine Oliver v0.3 envia e entrega no Telegram.
# Eventos: entrada, parcial, trailing, saida, conflito.


def _oliver_num(v):
    """Converte para float ou devolve None (aceita None, '', 'null', texto invalido)."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _oliver_preco(v) -> str:
    """'2651.85' -> '2.651,85' (preserva as casas decimais enviadas pelo Pine)."""
    if v is None or v == "":
        return "-"
    s = str(v)
    sinal = "-" if s.startswith("-") else ""
    s = s.lstrip("-")
    inteiro, _, dec = s.partition(".")
    grupos = []
    while len(inteiro) > 3:
        grupos.insert(0, inteiro[-3:])
        inteiro = inteiro[:-3]
    grupos.insert(0, inteiro)
    return sinal + ".".join(grupos) + ("," + dec if dec else "")


def _oliver_r(v) -> str:
    """0.14 -> '+0,14R'."""
    n = _oliver_num(v)
    return "-" if n is None else f"{n:+.2f}".replace(".", ",") + "R"


def _oliver_pct(v) -> str:
    """Percentual ja em escala 0-100 (pct1/pct2): 50.0 -> '50%'."""
    n = _oliver_num(v)
    return "-" if n is None else f"{n:g}".replace(".", ",") + "%"


def _oliver_prob(v) -> str:
    """Probabilidade em fracao 0-1 (prob_a1/a2/a3): 0.68 -> '68%'. Fora do intervalo -> '-'."""
    n = _oliver_num(v)
    return f"{n * 100:.0f}%" if n is not None and 0 <= n <= 1 else "-"


def _oliver_tf(tf) -> str:
    tf = str(tf or "")
    if tf.isdigit():
        n = int(tf)
        return f"{n // 60}h" if n >= 60 and n % 60 == 0 else f"{n}m"
    return tf


def _oliver_lista(v) -> str:
    return str(v or "").strip("|").replace("|", ", ") or "-"


def _oliver_linhas_probabilidade(p: dict) -> list:
    """Bloco de probabilidade historica da entrada (campos novos do Pine v0.3)."""
    status = str(p.get("status_amostra") or "").strip()
    tem_prob = any(p.get(k) is not None for k in ("prob_a1", "prob_a2", "prob_a3"))
    if not status and not tem_prob:
        return []  # payload sem os campos novos (ex.: Pine v0.2): nada a acrescentar

    linhas = [""]
    if "INSUFICIENTE" in status.upper():
        linhas.append(f"Probabilidade hist\u00f3rica: amostra insuficiente ({status})")
        return linhas

    linhas.append(
        "Probabilidade hist\u00f3rica: "
        f"A1 {_oliver_prob(p.get('prob_a1'))} | "
        f"A2 {_oliver_prob(p.get('prob_a2'))} | "
        f"A3 {_oliver_prob(p.get('prob_a3'))}"
    )
    amostra = status or (f"n={p['amostra']:g}" if _oliver_num(p.get("amostra")) is not None else "-")
    linhas.append(f"Amostra: {amostra}")
    if _oliver_num(p.get("er_final")) is not None:
        alvo_final = p.get("alvo_final")
        rotulo = f"E[R] no alvo final ({alvo_final})" if alvo_final else "E[R] no alvo final"
        linhas.append(f"{rotulo}: {_oliver_r(p.get('er_final'))}")
    return linhas


def _oliver_mensagem(p: dict) -> str:
    ev = str(p.get("evento", "")).lower()
    px = _oliver_preco
    cab = f"{p.get('ativo', '?')} \u00b7 {_oliver_tf(p.get('tf'))}"
    lado = p.get("lado", "")
    setup = p.get("setup", "")
    hora = p.get("hora", "")

    if ev == "entrada":
        emoji = "\U0001f7e2" if lado == "COMPRA" else "\U0001f534"
        pct_rest = max(0.0, 100.0 - (_oliver_num(p.get("pct1")) or 0.0) - (_oliver_num(p.get("pct2")) or 0.0))
        linhas = [
            f"{emoji} OLIVER \u2014 {lado}",
            f"{cab} \u00b7 {hora}",
            "",
            f"Setup: {setup}",
        ]
        if p.get("tipo_entrada"):
            linhas.append(f"Tipo de entrada: {p['tipo_entrada']}")
        linhas += [
            f"Gatilho: {px(p.get('gatilho'))}",
            f"Entrada: {px(p.get('entrada'))}",
            f"Stop: {px(p.get('stop'))} (R = {px(p.get('r'))})",
        ]
        if p.get("perfil_stop"):
            linhas.append(f"Perfil do stop: {p['perfil_stop']}")
        linhas += [
            "",
            f"\U0001f3af A1: {px(p.get('alvo1'))} ({_oliver_pct(p.get('pct1'))})",
            f"\U0001f3af A2: {px(p.get('alvo2'))} ({_oliver_pct(p.get('pct2'))})",
            f"\U0001f3af A3: {px(p.get('alvo3'))} (restante {_oliver_pct(pct_rest)})",
        ]
        if p.get("alvo_final"):
            linhas.append(f"Alvo final: {p['alvo_final']}")
        linhas += [
            "",
            f"Regime: {p.get('regime', '-')}",
            f"Contexto D/60/15: {p.get('contexto', '-')}",
        ]
        if p.get("confianca"):
            linhas.append(f"Confian\u00e7a: {p['confianca']} (contagem de crit\u00e9rios, n\u00e3o probabilidade)")
        linhas += _oliver_linhas_probabilidade(p)
        return "\n".join(linhas)

    if ev == "parcial":
        nivel = str(p.get("nivel", ""))
        linhas = [
            f"\U0001f7e1 OLIVER \u2014 PARCIAL {nivel} ({_oliver_pct(p.get('pct'))})",
            f"{cab} \u00b7 {lado} \u00b7 {setup} \u00b7 {hora}",
            f"Alvo {nivel[-1:]} atingido: {px(p.get('preco'))}",
        ]
        if p.get("stop") not in (None, ""):  # o Pine v0.3 nao envia 'stop' na parcial
            linhas.append(f"Stop atual: {px(p.get('stop'))}")
        return "\n".join(linhas)

    if ev == "trailing":
        return "\n".join([
            "\U0001f501 OLIVER \u2014 STOP AJUSTADO",
            f"{cab} \u00b7 {lado} \u00b7 {setup} \u00b7 {hora}",
            f"Stop: {px(p.get('stop_anterior'))} \u2192 {px(p.get('stop'))}",
            f"Perfil: {p.get('perfil', '-')} \u00b7 Entrada: {px(p.get('entrada'))}",
        ])

    if ev == "saida":
        motivo = str(p.get("motivo", ""))
        titulo = {
            "ALVO1": "\u2705 OLIVER \u2014 SA\u00cdDA ALVO 1",
            "ALVO2": "\u2705 OLIVER \u2014 SA\u00cdDA ALVO 2",
            "ALVO3": "\u2705 OLIVER \u2014 SA\u00cdDA ALVO 3",
            "STOP": "\u26d4 OLIVER \u2014 STOP",
            "STOP_AMBIGUO_PRIORIZADO": "\u26d4 OLIVER \u2014 STOP (AMB\u00cdGUO, PRIORIZADO)",
            "STOP_NA_BARRA_DA_ENTRADA": "\u26d4 OLIVER \u2014 STOP NA BARRA DA ENTRADA",
            "REVERSAO": "\U0001f504 OLIVER \u2014 SA\u00cdDA POR REVERS\u00c3O (VIRAR A M\u00c3O)",
        }.get(motivo, f"\u23f9 OLIVER \u2014 SA\u00cdDA ({motivo})")

        def marca(b):
            return "\u2714" if b else "\u2718"

        return "\n".join([
            titulo,
            f"{cab} \u00b7 {lado} \u00b7 {setup} \u00b7 {hora}",
            f"Entrada: {px(p.get('entrada'))} \u2192 Sa\u00edda: {px(p.get('saida'))}",
            f"Parciais: A1 {marca(p.get('p1'))} A2 {marca(p.get('p2'))}",
            f"Resultado do restante: {_oliver_r(p.get('r_restante'))}",
            f"Resultado com parciais: {_oliver_r(p.get('r_ponderado'))}",
        ])

    if ev == "conflito":
        modo = str(p.get("modo", "Bloquear"))
        # O Pine envia "Bloquear (v0.2)": comparar pelo prefixo, nao por igualdade.
        acao = "Bloqueado \u2014 verificar manualmente" if modo.startswith("Bloquear") else f"Resolvido por: {modo}"
        return "\n".join([
            "\u26a0\ufe0f OLIVER \u2014 CONFLITO",
            f"{cab} \u00b7 {hora}",
            f"Compra: {_oliver_lista(p.get('compras'))} | Venda: {_oliver_lista(p.get('vendas'))}",
            acao,
        ])

    return f"OLIVER \u2014 evento desconhecido: {ev}\n{json.dumps(p, ensure_ascii=False)}"


def _oliver_entregar(texto: str):
    try:
        _enviar_telegram(texto)
    except Exception as e:
        log_oliver.exception("Falha ao enviar Telegram (Oliver): %s", e)


@app.post("/webhook/oliver")
async def webhook_oliver(request: Request, background: BackgroundTasks):
    corpo = await request.body()
    log_oliver.info("Oliver recebido (%d bytes): %s", len(corpo), corpo[:300])
    try:
        payload = json.loads(corpo)
    except Exception:
        raise HTTPException(status_code=400, detail="JSON invalido")
    if WEBHOOK_SHARED_SECRET and payload.get("secret") != WEBHOOK_SHARED_SECRET:
        raise HTTPException(status_code=401, detail="Segredo do webhook invalido")
    if "evento" not in payload:
        raise HTTPException(status_code=422, detail="Campo 'evento' ausente")
    try:
        texto = _oliver_mensagem(payload)
    except Exception as e:
        # Um erro de formatacao nunca deve engolir o alerta: entrega o payload bruto.
        log_oliver.exception("Falha ao formatar mensagem Oliver: %s", e)
        texto = "OLIVER \u2014 falha ao formatar, payload bruto:\n" + json.dumps(payload, ensure_ascii=False)
    background.add_task(_oliver_entregar, texto)
    return {"status": "ok", "evento": payload["evento"]}
