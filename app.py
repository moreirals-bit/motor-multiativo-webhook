import logging
import os

import requests
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
WEBHOOK_SHARED_SECRET = os.environ.get("WEBHOOK_SHARED_SECRET", "")

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("oliver")

app = FastAPI(title="Oliver Velez - Webhook")


def _enviar_telegram(texto: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resposta = requests.post(
        url,
        data={"chat_id": TELEGRAM_CHAT_ID, "text": texto},
        timeout=10,
    )
    resposta.raise_for_status()


def _oliver_preco(valor) -> str:
    if valor is None or valor == "":
        return "-"

    texto = str(valor)
    sinal = "-" if texto.startswith("-") else ""
    texto = texto.lstrip("-")

    inteiro, separador, decimais = texto.partition(".")
    grupos = []

    while len(inteiro) > 3:
        grupos.insert(0, inteiro[-3:])
        inteiro = inteiro[:-3]

    grupos.insert(0, inteiro)
    resultado = ".".join(grupos)

    if separador:
        resultado += "," + decimais

    return sinal + resultado


def _oliver_r(valor) -> str:
    try:
        return f"{float(valor):+.2f}".replace(".", ",") + "R"
    except (TypeError, ValueError):
        return "-"


def _oliver_pct(valor) -> str:
    try:
        return f"{float(valor):g}".replace(".", ",") + "%"
    except (TypeError, ValueError):
        return "-"


def _oliver_tf(timeframe) -> str:
    timeframe = str(timeframe or "")

    if timeframe.isdigit():
        minutos = int(timeframe)
        if minutos >= 60 and minutos % 60 == 0:
            return f"{minutos // 60}h"
        return f"{minutos}m"

    return timeframe


def _oliver_lista(valor) -> str:
    return str(valor or "").strip("|").replace("|", ", ") or "-"


def _oliver_mensagem(payload: dict) -> str:
    evento = str(payload.get("evento", "")).lower()
    preco = _oliver_preco

    cabecalho = (
        f"{payload.get('ativo', '?')} · "
        f"{_oliver_tf(payload.get('tf'))}"
    )
    lado = payload.get("lado", "")
    setup = payload.get("setup", "")
    hora = payload.get("hora", "")

    if evento == "entrada":
        emoji = "🟢" if lado == "COMPRA" else "🔴"
        pct1 = float(payload.get("pct1") or 0)
        pct2 = float(payload.get("pct2") or 0)
        pct_restante = max(0.0, 100.0 - pct1 - pct2)

        return "\n".join([
            f"{emoji} OLIVER — {lado}",
            f"{cabecalho} · {hora}",
            "",
            f"Setup: {setup}",
            f"Gatilho: {preco(payload.get('gatilho'))}",
            f"Entrada: {preco(payload.get('entrada'))}",
            (
                f"Stop: {preco(payload.get('stop'))} "
                f"(R = {preco(payload.get('r'))})"
            ),
            "",
            (
                f"🎯 A1: {preco(payload.get('alvo1'))} "
                f"({_oliver_pct(payload.get('pct1'))})"
            ),
            (
                f"🎯 A2: {preco(payload.get('alvo2'))} "
                f"({_oliver_pct(payload.get('pct2'))})"
            ),
            (
                f"🎯 A3: {preco(payload.get('alvo3'))} "
                f"(restante {_oliver_pct(pct_restante)})"
            ),
            "",
            f"Regime: {payload.get('regime', '-')}",
            f"Contexto D/60/15: {payload.get('contexto', '-')}",
        ])

    if evento == "parcial":
        nivel = str(payload.get("nivel", ""))

        return "\n".join([
            (
                f"🟡 OLIVER — PARCIAL {nivel} "
                f"({_oliver_pct(payload.get('pct'))})"
            ),
            f"{cabecalho} · {lado} · {setup} · {hora}",
            (
                f"Alvo {nivel[-1:]} atingido: "
                f"{preco(payload.get('preco'))}"
            ),
            f"Stop atual: {preco(payload.get('stop'))}",
        ])

    if evento == "trailing":
        return "\n".join([
            "🔁 OLIVER — STOP AJUSTADO",
            f"{cabecalho} · {lado} · {setup} · {hora}",
            (
                f"Stop: {preco(payload.get('stop_anterior'))} "
                f"→ {preco(payload.get('stop'))}"
            ),
            (
                f"Perfil: {payload.get('perfil', '-')} "
                f"· Entrada: {preco(payload.get('entrada'))}"
            ),
        ])

    if evento == "saida":
        motivo = str(payload.get("motivo", ""))
        titulo = {
            "ALVO3": "✅ OLIVER — SAÍDA ALVO 3",
            "STOP": "⛔ OLIVER — STOP",
            "STOP_AMBIGUO_PRIORIZADO": (
                "⛔ OLIVER — STOP (AMBÍGUO, PRIORIZADO)"
            ),
        }.get(motivo, f"⏹ OLIVER — SAÍDA ({motivo})")

        def marca(valor):
            return "✔" if valor else "✘"

        return "\n".join([
            titulo,
            f"{cabecalho} · {lado} · {setup} · {hora}",
            (
                f"Entrada: {preco(payload.get('entrada'))} "
                f"→ Saída: {preco(payload.get('saida'))}"
            ),
            (
                f"Parciais: A1 {marca(payload.get('p1'))} "
                f"A2 {marca(payload.get('p2'))}"
            ),
            (
                "Resultado do restante: "
                f"{_oliver_r(payload.get('r_restante'))}"
            ),
            (
                "Resultado com parciais: "
                f"{_oliver_r(payload.get('r_ponderado'))}"
            ),
        ])

    if evento == "conflito":
        modo = payload.get("modo", "Bloquear")
        acao = (
            "Bloqueado — verificar manualmente"
            if modo == "Bloquear"
            else f"Resolvido por: {modo}"
        )

        return "\n".join([
            "⚠️ OLIVER — CONFLITO",
            f"{cabecalho} · {hora}",
            (
                f"Compra: {_oliver_lista(payload.get('compras'))} "
                f"| Venda: {_oliver_lista(payload.get('vendas'))}"
            ),
            acao,
        ])

    return f"OLIVER — evento desconhecido: {evento}"


def _oliver_entregar(texto: str):
    try:
        _enviar_telegram(texto)
    except Exception:
        log.exception("Falha ao enviar mensagem Oliver ao Telegram")


@app.post("/webhook/oliver")
async def webhook_oliver(
    request: Request,
    background: BackgroundTasks,
):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON inválido")

    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=400,
            detail="O payload precisa ser um objeto JSON",
        )

    if WEBHOOK_SHARED_SECRET and payload.get("secret") != WEBHOOK_SHARED_SECRET:
        raise HTTPException(
            status_code=401,
            detail="Segredo do webhook inválido",
        )

    if "evento" not in payload:
        raise HTTPException(
            status_code=422,
            detail="Campo 'evento' ausente",
        )

    log.info(
        "Oliver recebido: evento=%s ativo=%s",
        payload.get("evento"),
        payload.get("ativo", "não informado"),
    )

    mensagem = _oliver_mensagem(payload)
    background.add_task(_oliver_entregar, mensagem)

    return {
        "status": "ok",
        "evento": payload["evento"],
    }


@app.get("/health")
async def health():
    return {"status": "ok"}
