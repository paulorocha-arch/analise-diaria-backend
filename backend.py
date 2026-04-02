#!/usr/bin/env python3
"""
Análise Diária — Backend
Serve dados do Qlik Cloud (app Farol) para o frontend no Netlify.
Iniciar: python backend.py
"""

import asyncio, json, os, sys, time
from datetime import datetime, timezone
from flask import Flask, jsonify, request
from flask_cors import CORS
import httpx, websockets

# ── .env ────────────────────────────────────────────────────────────────────
_env = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(_env):
    with open(_env) as f:
        for line in f:
            line = line.strip()
            if line and "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                if not os.environ.get(k.strip()):
                    os.environ[k.strip()] = v.strip()

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

TENANT  = os.environ.get("QLIK_TENANT", "bigbox.us.qlikcloud.com")
API_KEY = os.environ.get("QLIK_API_KEY", "")
APP_FAROL = "bd053bfe-11b0-4f35-ab25-d316e23f8977"

app = Flask(__name__)
CORS(app, origins="*")

# Cache em memória: {cache_key: (timestamp, result)}
_cache = {}
_CACHE_TTL = 300  # 5 minutos

# ── Helpers Qlik Engine API ──────────────────────────────────────────────────

async def _engine_session(app_id, callback):
    uri = f"wss://{TENANT}/app/{app_id}"
    async with websockets.connect(
        uri,
        additional_headers={"Authorization": f"Bearer {API_KEY}"},
        open_timeout=20,
    ) as ws:
        msg_id = 0

        async def rpc(method, params, handle=-1):
            nonlocal msg_id
            msg_id += 1
            _id = msg_id
            await ws.send(json.dumps({
                "jsonrpc": "2.0", "id": _id,
                "method": method, "handle": handle, "params": params
            }))
            return _id

        async def recv_until(target_id, timeout=45):
            deadline = asyncio.get_event_loop().time() + timeout
            while True:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    return None
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                    msg = json.loads(raw)
                    if msg.get("id") == target_id:
                        return msg
                except (asyncio.TimeoutError, websockets.ConnectionClosed):
                    return None

        open_id  = await rpc("OpenDoc", [app_id])
        open_msg = await recv_until(open_id, timeout=60)
        if not open_msg:
            raise RuntimeError("Timeout ao abrir app")
        doc_handle = open_msg.get("result", {}).get("qReturn", {}).get("qHandle")
        if doc_handle is None:
            raise RuntimeError(f"Erro ao abrir app: {open_msg.get('error')}")
        return await callback(rpc, recv_until, doc_handle)


async def _hypercube(app_id, dimensions, measures, rows=100):
    q_dims  = [{"qDef": {"qFieldDefs": [d]}} for d in dimensions]
    q_meas  = [{"qDef": {"qDef": m}} for m in measures]
    n_cols  = len(dimensions) + len(measures)
    col_names = dimensions + measures

    async def _run(rpc, recv_until, doc_handle):
        obj_def = {
            "qInfo": {"qType": "HyperCube"},
            "qHyperCubeDef": {
                "qDimensions": q_dims,
                "qMeasures":   q_meas,
                "qInitialDataFetch": [{"qTop": 0, "qLeft": 0, "qHeight": rows, "qWidth": n_cols}],
                "qMode": "S",
                "qSuppressZero": False,
                "qSuppressMissing": False,
            },
        }
        cid = await rpc("CreateSessionObject", [obj_def], doc_handle)
        msg = await recv_until(cid)
        if not msg:
            raise RuntimeError("Timeout ao criar hipercubo")
        handle = msg.get("result", {}).get("qReturn", {}).get("qHandle")
        lid = await rpc("GetLayout", [], handle)
        msg = await recv_until(lid, timeout=30)
        if not msg:
            raise RuntimeError("Timeout ao obter dados")
        hc    = msg.get("result", {}).get("qLayout", {}).get("qHyperCube", {})
        pages = hc.get("qDataPages", [])
        data  = []
        for page in pages:
            for row in page.get("qMatrix", []):
                row_data = {}
                for i, cell in enumerate(row):
                    if i < len(col_names):
                        row_data[col_names[i]] = cell.get("qText", str(cell.get("qNum", "")))
                data.append(row_data)
        return data

    return await _engine_session(app_id, _run)


def _run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _float(val):
    if not val or val in ("-", ""):
        return 0.0
    try:
        return float(str(val).replace(".", "").replace(",", ".").replace("%", "").strip())
    except Exception:
        return 0.0


def _mes_ano(ano, mes):
    nomes = {1:"jan",2:"fev",3:"mar",4:"abr",5:"mai",6:"jun",
             7:"jul",8:"ago",9:"set",10:"out",11:"nov",12:"dez"}
    return f"{nomes[mes]} {ano}"


def _days(d1, d2):
    return ",".join(str(i) for i in range(d1, d2 + 1))


# ── Endpoints ────────────────────────────────────────────────────────────────

@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "tenant": TENANT, "api_key": bool(API_KEY)})


@app.route("/api/vendas", methods=["POST"])
def api_vendas():
    """
    Body JSON:
      ano      : int  (ex: 2026)
      mes      : int  (1-12)
      dia_ini  : int  (1)
      dia_fim  : int  (ultimo dia selecionado)
      empresas : list[str]  ([] = todas)
      bandeiras: list[str]  ([] = todas)
    """
    body     = request.json or {}
    ano      = int(body.get("ano", datetime.now().year))
    mes      = int(body.get("mes", datetime.now().month))
    dia_ini  = int(body.get("dia_ini", 1))
    dia_fim  = int(body.get("dia_fim", datetime.now().day - 1))
    empresas = body.get("empresas", [])   # lista de nomes de empresa
    bandeiras= body.get("bandeiras", [])  # ["BIG BOX","ULT"]

    if dia_ini > dia_fim:
        dia_ini, dia_fim = dia_fim, dia_ini

    # Cache lookup
    cache_key = f"{ano}-{mes}-{dia_ini}-{dia_fim}-{sorted(empresas)}-{sorted(bandeiras)}"
    cached = _cache.get(cache_key)
    if cached:
        ts, payload = cached
        if time.time() - ts < _CACHE_TTL:
            return jsonify(payload)

    ma      = _mes_ano(ano, mes)
    ma_aa   = _mes_ano(ano - 1, mes)   # ano anterior
    days    = _days(dia_ini, dia_fim)
    days_aa = days  # mesmos dias no ano anterior

    # Filtro de empresa via set analysis
    def emp_filter(extra=""):
        parts = [f"FlagFatosVendas={{1}},[Mês/Ano]={{'{ma}'}},Dia={{{days}}}{extra}"]
        return parts[0]

    def meta_filter(extra=""):
        return f"FlagFatosMetas={{1}},[Mês/Ano]={{'{ma}'}},Dia={{{days}}}{extra}"

    # Expressões globais (sem filtro de empresa — por loja)
    V_ACUM  = f"Sum({{<FlagFatosVendas={{1}},[Mês/Ano]={{'{ma}'}},Dia={{{days}}}>}} #Medida1)"
    M_ACUM  = f"Sum({{<FlagFatosMetas={{1}},[Mês/Ano]={{'{ma}'}},Dia={{{days}}}>}} #Medida1)"
    # Ano anterior (mesmos dias)
    V_AA    = f"Sum({{<FlagFatosVendas={{1}},[Mês/Ano]={{'{ma_aa}'}},Dia={{{days_aa}}}>}} #Medida1)"
    # Meta mês completo (para Prog. ML%)
    M_MES   = f"Sum({{<FlagFatosMetas={{1}},[Mês/Ano]={{'{ma}'}}>}} #Medida1)"
    # Clientes (NFs distintas)
    NRO_CL  = f"Count({{<FlagFatosVendas={{1}},[Mês/Ano]={{'{ma}'}},Dia={{{days}}}>}} Distinct ChaveNF)"
    NRO_CL_AA = f"Count({{<FlagFatosVendas={{1}},[Mês/Ano]={{'{ma_aa}'}},Dia={{{days_aa}}}>}} Distinct ChaveNF)"
    # Margem PDV
    MRG_PDV = f"Sum({{<FlagFatosVendas={{1}},[Mês/Ano]={{'{ma}'}},Dia={{{days}}}>}} #Medida2)"
    M_MRG   = f"Sum({{<FlagFatosMetas={{1}},[Mês/Ano]={{'{ma}'}},Dia={{{days}}}>}} #Medida2)"
    # Quebra
    QUEBRA  = f"Sum({{<FlagFatosQuebras={{1}},[Mês/Ano]={{'{ma}'}},Dia={{{days}}}>}} #Medida1)"
    M_QUEBRA= f"Sum({{<FlagFatosMetasQuebras={{1}},[Mês/Ano]={{'{ma}'}},Dia={{{days}}}>}} #Medida1)"

    dias_passados = dia_fim - dia_ini + 1
    try:
        from calendar import monthrange
        dias_no_mes = monthrange(ano, mes)[1]
    except Exception:
        dias_no_mes = 30

    try:
        raw = _run_async(_hypercube(
            APP_FAROL,
            ["Empresa"],
            [V_ACUM, M_ACUM, V_AA, M_MES, NRO_CL, NRO_CL_AA, MRG_PDV, M_MRG, QUEBRA, M_QUEBRA],
            rows=60,
        ))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    rows_out = []
    for row in raw:
        empresa = row.get("Empresa", "").strip()
        if not empresa or empresa == "-":
            continue

        # Filtro de bandeira
        bandeira = "BIG BOX" if empresa.upper().startswith("BIG") else "ULT"
        if bandeiras and bandeira not in bandeiras:
            continue
        if empresas and empresa not in empresas:
            continue

        va   = _float(row.get(V_ACUM))
        ma   = _float(row.get(M_ACUM))
        v_aa = _float(row.get(V_AA))
        m_mes= _float(row.get(M_MES))
        nc   = _float(row.get(NRO_CL))
        nc_aa= _float(row.get(NRO_CL_AA))
        mrg  = _float(row.get(MRG_PDV))
        m_mrg= _float(row.get(M_MRG))
        qbr  = _float(row.get(QUEBRA))
        m_qbr= _float(row.get(M_QUEBRA))

        tm    = round(va / nc,  2) if nc   > 0 else 0
        tm_aa = round(v_aa / nc_aa, 2) if nc_aa > 0 else 0

        # Prog. Venda ML% = projeção linear / meta mês
        prog_venda = round((va / dias_passados * dias_no_mes / m_mes * 100) if m_mes > 0 and dias_passados > 0 else 0, 2)
        # Prog. Clientes ML%
        prog_cl    = round((nc / dias_passados * dias_no_mes / (nc_aa or 1) * 100 - 100) if dias_passados > 0 else 0, 2)
        # Prog. Ticket Médio ML%
        prog_tm    = round(((tm / tm_aa - 1) * 100) if tm_aa > 0 else 0, 2)
        # % Cresc Nro Clientes
        cresc_cl   = round(((nc / nc_aa - 1) * 100) if nc_aa > 0 else 0, 2)
        # % Cresc Ticket
        cresc_tm   = round(((tm / tm_aa - 1) * 100) if tm_aa > 0 else 0, 2)
        # Atingimento %
        ating      = round((va / ma * 100) if ma > 0 else 0, 2)
        ating_mrg  = round((mrg / m_mrg * 100) if m_mrg > 0 else 0, 2)

        rows_out.append({
            "empresa":       empresa,
            "bandeira":      bandeira,
            # Tabela
            "meta_venda":    round(ma, 2),
            "venda":         round(va, 2),
            "desvio":        round(va - ma, 2),
            "nro_clientes":  int(nc),
            "prog_cl_ml":    prog_cl,
            "ticket_medio":  tm,
            "prog_tm_ml":    prog_tm,
            "prog_venda_ml": prog_venda,
            "ating":         ating,
            # KPIs extras
            "cresc_cl":      cresc_cl,
            "cresc_tm":      cresc_tm,
            "margem_pdv":    round(mrg, 2),
            "meta_margem":   round(m_mrg, 2),
            "ating_margem":  ating_mrg,
            "quebra":        round(qbr, 2),
            "meta_quebra":   round(m_qbr, 2),
        })

    rows_out.sort(key=lambda x: x["empresa"])

    # Totais
    def _sum(k): return round(sum(r[k] for r in rows_out), 2)
    def _sumint(k): return sum(r[k] for r in rows_out)

    tot_va   = _sum("venda")
    tot_ma   = _sum("meta_venda")
    tot_nc   = _sumint("nro_clientes")
    tot_mrg  = _sum("margem_pdv")
    tot_mmrg = _sum("meta_margem")
    tot_qbr  = _sum("quebra")
    tot_mqbr = _sum("meta_quebra")
    tot_tm   = round(tot_va / tot_nc, 2) if tot_nc > 0 else 0

    totais = {
        "empresa":       "TOTAIS",
        "bandeira":      "",
        "meta_venda":    tot_ma,
        "venda":         tot_va,
        "desvio":        round(tot_va - tot_ma, 2),
        "nro_clientes":  tot_nc,
        "ticket_medio":  tot_tm,
        "prog_cl_ml":    0,
        "prog_tm_ml":    0,
        "prog_venda_ml": round((tot_va / dias_passados * dias_no_mes / tot_ma * 100) if tot_ma > 0 and dias_passados > 0 else 0, 2),
        "ating":         round((tot_va / tot_ma * 100) if tot_ma > 0 else 0, 2),
        "cresc_cl":      0,
        "cresc_tm":      0,
        "margem_pdv":    tot_mrg,
        "meta_margem":   tot_mmrg,
        "ating_margem":  round((tot_mrg / tot_mmrg * 100) if tot_mmrg > 0 else 0, 2),
        "quebra":        tot_qbr,
        "meta_quebra":   tot_mqbr,
    }

    payload = {
        "filtros": {"ano": ano, "mes": mes, "dia_ini": dia_ini, "dia_fim": dia_fim,
                    "dias_passados": dias_passados, "dias_no_mes": dias_no_mes},
        "totais":  totais,
        "data":    rows_out,
    }
    _cache[cache_key] = (time.time(), payload)
    return jsonify(payload)


if __name__ == "__main__":
    print(f"Backend Analise Diaria — http://localhost:5001")
    print(f"Tenant: {TENANT}  |  API Key: {'OK' if API_KEY else 'NAO DEFINIDA'}")
    app.run(debug=False, host="0.0.0.0", port=5001)
