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


# --------------------------- FORMATACAO (v1.5) ---------------------------
def _num(v):
    """Numero valido ou None. O Pine (v1.4 e v1.5) manda 0 quando o valor nao existe (na); por isso 0 conta como ausente."""
    if v is None or isinstance(v, bool):
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if x != x or x == 0.0:
        return None
    return x


def _tf_txt(tf) -> str:
    """'60' -> '1h', '240' -> '4h', '15' -> '15m'; 'D', 'W' etc. passam sem mudar."""
    if tf is None:
        return ""
    s = str(tf).strip()
    if s.isdigit():
        m = int(s)
        return f"{m // 60}h" if m >= 60 and m % 60 == 0 else f"{m}m"
    return s


def _fmt_preco(v) -> str:
    """Casas por magnitude (mesma regra do f_px do Pine): >=100 -> 2; >=1 -> 4; >=0.01 -> 5; senao 8. Minimo 2 casas."""
    x = _num(v)
    if x is None:
        return "-"
    ax = abs(x)
    casas = 2 if ax >= 100 else 4 if ax >= 1 else 5 if ax >= 0.01 else 8
    inteiro, _, dec = f"{x:.{casas}f}".partition(".")
    return f"{inteiro}.{dec.rstrip('0').ljust(2, '0')}"


def _mult_r(alvo, entrada, stop):
    """Distancia do alvo em multiplos do risco (R = |entrada - stop|); None se nao der para calcular."""
    a, e, s = _num(alvo), _num(entrada), _num(stop)
    if a is None or e is None or s is None or e == s:
        return None
    return abs(a - e) / abs(e - s)


_NIVEL_CONF = {"GRANDE": "GRANDE", "MEDIA": "MÃ‰DIA", "BAIXA": "BAIXA"}
_ITEM_CONF = {
    "score": "score forte",
    "timing": "timing confirmado",
    "amostra": "amostra mÃ­nima",
    "ExpR": "expectÃ¢ncia do plano > 0",
    "prob1": "prob. do Alvo 1",
}


# --------------------------- NARRATIVA (numeros antes da frase, sempre) ---------------------------
def _montar_mensagem(payload: dict, dentro_do_teto: bool, risco_pos_sinal: float) -> str:
    ativo = payload["ativo"]
    eh_compra = payload["direcao"].lower().startswith("c") or payload["direcao"].lower() == "buy"
    direcao = "COMPRA" if eh_compra else "VENDA"
    icone = "ðŸŸ¢" if eh_compra else "ðŸ”´"
    score = payload["score"]
    timing = "confirmado" if payload["timing_confirmado"] else "pendente"
    amostra_atual = payload["amostra_atual"]
    amostra_exigida = payload["amostra_exigida"]
    taxa = payload["taxa_acerto"]
    expect = payload["expectancia"]
    kelly = payload["kelly_pct"]
    aviso_classe = payload.get("aviso_classe")  # ex.: "sem leitura fundamentalista/sazonal (softs)"
    # Todos os campos abaixo sao OPCIONAIS (via .get()): alertas antigos (v1.4 ou anteriores) continuam funcionando.
    tf = _tf_txt(payload.get("timeframe"))
    tf_mare = _tf_txt(payload.get("tf_mare"))  # so existe a partir do Pine v1.5

    # 1) cabecalho: direcao, ativo e tempo grafico analisado
    cabecalho = f"{icone} {direcao} â€” {ativo}"
    if tf:
        cabecalho += f" | grÃ¡fico {tf}"
        if tf_mare:
            cabecalho += f" (marÃ© {tf_mare})"
    if amostra_atual < amostra_exigida:
        cabecalho += " â€” SÃ“ OBSERVAÃ‡ÃƒO (amostra curta)"
    linhas = [cabecalho]

    # 2) confianca: checklist do motor, NAO e probabilidade de lucro (so v1.5)
    nivel = _NIVEL_CONF.get(str(payload.get("confianca", "")).upper())
    if nivel:
        pts = payload.get("confianca_pontos")
        pts_txt = f" ({pts}/5)" if isinstance(pts, (int, float)) and not isinstance(pts, bool) else ""
        linha = f"ConfianÃ§a: {nivel}{pts_txt} â€” checklist do motor, nÃ£o Ã© chance de lucro."
        faltam = [_ITEM_CONF.get(x, x) for x in str(payload.get("confianca_faltam", "")).split() if x]
        if faltam:
            linha += " Falta: " + ", ".join(faltam) + "."
        linhas.append(linha)

    # 3) plano de trade: entrada e stop (rompimento / lado oposto do candle do sinal)
    entrada = _num(payload.get("entrada"))
    stop = _num(payload.get("stop"))
    if entrada is not None and stop is not None:
        if eh_compra:
            linhas.append(f"Entrada: comprar ao romper {_fmt_preco(entrada)} (mÃ¡xima do candle do sinal)")
        else:
            linhas.append(f"Entrada: vender ao romper {_fmt_preco(entrada)} (mÃ­nima do candle do sinal)")
        linhas.append(f"Stop: {_fmt_preco(stop)} (lado oposto do mesmo candle)")

    # 4) saidas parciais e saida final
    alvo1 = _num(payload.get("alvo1"))
    alvo2 = _num(payload.get("alvo2"))
    if alvo1 is not None and alvo2 is not None:
        amostra_alvos_suf = payload.get("amostra_alvos_suficiente", False)
        prob_alvo1 = payload.get("prob_alvo1")
        prob_alvo2 = payload.get("prob_alvo2")
        prob1_txt = f"{prob_alvo1:.0%}" if amostra_alvos_suf and prob_alvo1 is not None else "amostra insuficiente"
        prob2_txt = f"{prob_alvo2:.0%}" if amostra_alvos_suf and prob_alvo2 is not None else "amostra insuficiente"
        pct1 = payload.get("pct_parcial1")
        pct2 = payload.get("pct_parcial2")
        r1 = _mult_r(alvo1, entrada, stop)
        r2 = _mult_r(alvo2, entrada, stop)
        r1_txt = f"{r1:g}R, " if r1 is not None else ""
        r2_txt = f"{r2:g}R, " if r2 is not None else ""
        acao1 = f" â†’ realizar {pct1:.0f}%" if pct1 is not None else ""
        acao2 = f" â†’ realizar {pct2:.0f}%" if pct2 is not None else ""
        linhas.append(f"Parcial 1 (Alvo 1): {_fmt_preco(alvo1)} ({r1_txt}prob. histÃ³rica: {prob1_txt}){acao1}")
        linhas.append(f"Parcial 2 (Alvo 2): {_fmt_preco(alvo2)} ({r2_txt}prob. histÃ³rica: {prob2_txt}){acao2}")
        if pct1 is not None and pct2 is not None:
            resto = max(0.0, 100.0 - float(pct1) - float(pct2))
            linhas.append(
                f"SaÃ­da final ({resto:.0f}%): depois da Parcial 1, stop na entrada; sair do restante quando o painel do "
                f"indicador mostrar SAÃDA (Kick ADX, ADX fraco, BB fechando, TRIX/EstocÃ¡stico contra) ou no stop."
            )
            linhas.append(
                f"Parciais {pct1:.0f}/{pct2:.0f}/{resto:.0f}% e alvos em R: sugestÃ£o prÃ³pria editÃ¡vel â€” NÃƒO Ã© regra documentada "
                f"de Velez/Didi (sÃ³ o CONCEITO de 'sair em partes' Ã© real)."
            )

    # 5) preco medio (2a entrada OPCIONAL, so v1.5)
    preco_medio = _num(payload.get("preco_medio"))
    if preco_medio is not None:
        linha = f"PreÃ§o mÃ©dio (opcional): {_fmt_preco(preco_medio)}, mesmo stop"
        if entrada is not None:
            linha += f". Dividindo o tamanho meio a meio, a posiÃ§Ã£o fica em â‰ˆ {_fmt_preco((entrada + preco_medio) / 2)}"
        linha += ". SugestÃ£o prÃ³pria, sem teste; nunca some tamanho alÃ©m do Kelly."
        linhas.append(linha)

    # 6) numeros do motor (como antes)
    linhas += [
        # Score: v1 era inteiro 0-4; na v1.2+ o score e continuo e pode passar de 4 - por isso so mostra o numero.
        f"Score: {score:.2f} | Timing (Velez): {timing}",
        f"Amostra: {amostra_atual}/{amostra_exigida} | Taxa histÃ³rica: {taxa:.0%} | ExpectÃ¢ncia: {expect:.2f}R",
        f"Tamanho sugerido (Kelly ajustado): {kelly:.2f}% do capital",
    ]

    if not dentro_do_teto:
        linhas.append(
            f"âš ï¸ Fora do teto de risco agregado ({risco_pos_sinal:.1f}% > {TETO_RISCO_AGREGADO_PCT:.1f}%) "
            f"â€” sinal informativo, nÃ£o somar posiÃ§Ã£o nova agora."
        )
    if aviso_classe:
        linhas.append(f"Aviso: {aviso_classe}")
    if amostra_atual < amostra_exigida:
        linhas.append("Aviso: amostra ainda insuficiente â€” tratar como observaÃ§Ã£o, nÃ£o como sinal validado.")

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
