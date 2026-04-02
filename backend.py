#!/usr/bin/env python3
"""
Análise Diária — Backend
Arquitetura: Qlik Cloud → Firestore (cache) → Dashboard
Iniciar: python backend.py
"""

import asyncio, base64, hashlib, json, os, sys, time
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

TENANT    = os.environ.get("QLIK_TENANT", "bigbox.us.qlikcloud.com")
API_KEY   = os.environ.get("QLIK_API_KEY", "")
APP_FAROL = "bd053bfe-11b0-4f35-ab25-d316e23f8977"

app = Flask(__name__)
CORS(app, origins="*")

# ── Firestore ────────────────────────────────────────────────────────────────
_db         = None
FIREBASE_OK = False
FIRESTORE_TTL = 3600   # 1 hora — dados do Firestore considerados frescos

try:
    import firebase_admin
    from firebase_admin import credentials, firestore as _fs

    # Prioridade 1: variável de ambiente FIREBASE_KEY_JSON (base64) → Render
    _key_b64 = os.environ.get("FIREBASE_KEY_JSON", "")
    # Prioridade 2: arquivo local (desenvolvimento)
    _key_file = r"C:\Users\paulorocha\qlik-dashboard\firebase-key.json"

    if _key_b64:
        _key_data = json.loads(base64.b64decode(_key_b64).decode())
        cred = credentials.Certificate(_key_data)
    elif os.path.exists(_key_file):
        cred = credentials.Certificate(_key_file)
    else:
        cred = None

    if cred and not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
        _db = _fs.client()
        FIREBASE_OK = True
        print("Firestore: OK")
    elif cred:
        _db = _fs.client()
        FIREBASE_OK = True
        print("Firestore: OK (app ja inicializado)")
    else:
        print("Firestore: chave nao encontrada — usando apenas cache em memoria")
except Exception as _e:
    print(f"Firestore: erro na inicializacao — {_e}")


def _fs_key(params: dict) -> str:
    raw = json.dumps(params, sort_keys=True)
    return hashlib.md5(raw.encode()).hexdigest()


def _fs_get(doc_id: str):
    """Lê do Firestore. Retorna payload ou None se expirado/ausente."""
    if not FIREBASE_OK:
        return None
    try:
        doc = _db.collection("cache_vendas").document(doc_id).get()
        if doc.exists:
            d = doc.to_dict()
            if time.time() - d.get("saved_at", 0) < FIRESTORE_TTL:
                return d.get("payload")
    except Exception as e:
        print(f"Firestore get erro: {e}")
    return None


def _fs_set(doc_id: str, payload: dict, meta: dict = None):
    """Salva no Firestore."""
    if not FIREBASE_OK:
        return
    try:
        doc = {"payload": payload, "saved_at": time.time(),
               "updated_at": datetime.now(timezone.utc).isoformat()}
        if meta:
            doc["meta"] = meta
        _db.collection("cache_vendas").document(doc_id).set(doc)
    except Exception as e:
        print(f"Firestore set erro: {e}")


# ── Cache em memória (camada rápida na frente do Firestore) ──────────────────
_mem_cache   = {}
_MEM_TTL     = 300   # 5 minutos


def _mem_get(key):
    entry = _mem_cache.get(key)
    if entry and time.time() - entry[0] < _MEM_TTL:
        return entry[1]
    return None


def _mem_set(key, val):
    _mem_cache[key] = (time.time(), val)


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
    q_dims    = [{"qDef": {"qFieldDefs": [d]}} for d in dimensions]
    q_meas    = [{"qDef": {"qDef": m}} for m in measures]
    n_cols    = len(dimensions) + len(measures)
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


def _run_async_all(*coros):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(asyncio.gather(*coros))
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


def _set_extra(compradores, departamentos, secoes):
    parts = []
    if compradores:
        vals = ",".join("'" + v + "'" for v in compradores)
        parts.append("Comprador={" + vals + "}")
    if departamentos:
        vals = ",".join("'" + v + "'" for v in departamentos)
        parts.append("[Nível 1]={" + vals + "}")
    if secoes:
        vals = ",".join("'" + v + "'" for v in secoes)
        parts.append("[Nível 2]={" + vals + "}")
    return ("," + ",".join(parts)) if parts else ""


# ── Endpoints ────────────────────────────────────────────────────────────────

@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "tenant": TENANT,
                    "api_key": bool(API_KEY), "firestore": FIREBASE_OK})


@app.route("/api/filtros")
def api_filtros():
    mem = _mem_get("__filtros__")
    if mem:
        return jsonify(mem)
    fs_data = _fs_get("__filtros__")
    if fs_data:
        _mem_set("__filtros__", fs_data)
        return jsonify(fs_data)
    try:
        comp_raw, dept_raw, sec_raw = _run_async_all(
            _hypercube(APP_FAROL, ["Comprador"], [], rows=100),
            _hypercube(APP_FAROL, ["Nível 1"],   [], rows=20),
            _hypercube(APP_FAROL, ["Nível 2"],   [], rows=50),
        )
        compradores   = sorted([r["Comprador"].strip() for r in comp_raw
                                if r.get("Comprador","").strip()
                                and r.get("Comprador") != "<Não Identificado>"])
        departamentos = sorted([r["Nível 1"].strip() for r in dept_raw
                                if r.get("Nível 1","").strip()])
        secoes        = sorted([r["Nível 2"].strip() for r in sec_raw
                                if r.get("Nível 2","").strip()])
        payload = {"compradores": compradores, "departamentos": departamentos, "secoes": secoes}
        _fs_set("__filtros__", payload)
        _mem_set("__filtros__", payload)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/vendas", methods=["POST"])
def api_vendas():
    body          = request.json or {}
    ano           = int(body.get("ano",  datetime.now().year))
    mes           = int(body.get("mes",  datetime.now().month))
    dia_ini       = int(body.get("dia_ini", 1))
    dia_fim       = int(body.get("dia_fim", datetime.now().day - 1))
    empresas      = body.get("empresas", [])
    bandeiras     = body.get("bandeiras", [])
    compradores_f = body.get("compradores", [])
    departamentos = body.get("departamentos", [])
    secoes        = body.get("secoes", [])

    if dia_ini > dia_fim:
        dia_ini, dia_fim = dia_fim, dia_ini

    params = {
        "ano": ano, "mes": mes, "dia_ini": dia_ini, "dia_fim": dia_fim,
        "empresas": sorted(empresas), "bandeiras": sorted(bandeiras),
        "compradores": sorted(compradores_f),
        "departamentos": sorted(departamentos),
        "secoes": sorted(secoes),
    }
    mem_key = json.dumps(params, sort_keys=True)
    fs_doc  = _fs_key(params)

    # 1. Cache em memória (mais rápido)
    mem = _mem_get(mem_key)
    if mem:
        return jsonify(mem)

    # 2. Firestore (persiste entre reinicializações do Render)
    fs_data = _fs_get(fs_doc)
    if fs_data:
        _mem_set(mem_key, fs_data)
        return jsonify(fs_data)

    # 3. Consulta Qlik
    ma      = _mes_ano(ano, mes)
    ma_aa   = _mes_ano(ano - 1, mes)
    days    = _days(dia_ini, dia_fim)
    days_aa = days
    xtra    = _set_extra(compradores_f, departamentos, secoes)

    V_ACUM    = f"Sum({{<FlagFatosVendas={{1}},[Mês/Ano]={{'{ma}'}},Dia={{{days}}}{xtra}>}} #Medida1)"
    M_ACUM    = f"Sum({{<FlagFatosMetas={{1}},[Mês/Ano]={{'{ma}'}},Dia={{{days}}}{xtra}>}} #Medida1)"
    V_AA      = f"Sum({{<FlagFatosVendas={{1}},[Mês/Ano]={{'{ma_aa}'}},Dia={{{days_aa}}}{xtra}>}} #Medida1)"
    M_MES     = f"Sum({{<FlagFatosMetas={{1}},[Mês/Ano]={{'{ma}'}}{xtra}>}} #Medida1)"
    NRO_CL    = f"Count({{<FlagFatosVendas={{1}},[Mês/Ano]={{'{ma}'}},Dia={{{days}}}{xtra}>}} Distinct ChaveNF)"
    NRO_CL_AA = f"Count({{<FlagFatosVendas={{1}},[Mês/Ano]={{'{ma_aa}'}},Dia={{{days_aa}}}{xtra}>}} Distinct ChaveNF)"
    MRG_PDV   = f"Sum({{<FlagFatosVendas={{1}},[Mês/Ano]={{'{ma}'}},Dia={{{days}}}{xtra}>}} #Medida2)"
    M_MRG     = f"Sum({{<FlagFatosMetas={{1}},[Mês/Ano]={{'{ma}'}},Dia={{{days}}}{xtra}>}} #Medida2)"
    QUEBRA    = f"Sum({{<FlagFatosQuebras={{1}},[Mês/Ano]={{'{ma}'}},Dia={{{days}}}>}} #Medida1)"
    M_QUEBRA  = f"Sum({{<FlagFatosMetasQuebras={{1}},[Mês/Ano]={{'{ma}'}},Dia={{{days}}}>}} #Medida1)"

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
        bandeira = "BIG BOX" if empresa.upper().startswith("BIG") else "ULT"
        if bandeiras and bandeira not in bandeiras:
            continue
        if empresas and empresa not in empresas:
            continue

        va    = _float(row.get(V_ACUM))
        ma_v  = _float(row.get(M_ACUM))
        v_aa  = _float(row.get(V_AA))
        m_mes = _float(row.get(M_MES))
        nc    = _float(row.get(NRO_CL))
        nc_aa = _float(row.get(NRO_CL_AA))
        mrg   = _float(row.get(MRG_PDV))
        m_mrg = _float(row.get(M_MRG))
        qbr   = _float(row.get(QUEBRA))
        m_qbr = _float(row.get(M_QUEBRA))

        tm    = round(va / nc,     2) if nc    > 0 else 0
        tm_aa = round(v_aa / nc_aa,2) if nc_aa > 0 else 0

        prog_venda = round((va / dias_passados * dias_no_mes / m_mes * 100)   if m_mes > 0 and dias_passados > 0 else 0, 2)
        prog_cl    = round((nc / dias_passados * dias_no_mes / (nc_aa or 1) * 100 - 100) if dias_passados > 0 else 0, 2)
        prog_tm    = round(((tm / tm_aa - 1) * 100)   if tm_aa > 0 else 0, 2)
        cresc_cl   = round(((nc / nc_aa - 1) * 100)   if nc_aa > 0 else 0, 2)
        cresc_tm   = round(((tm / tm_aa - 1) * 100)   if tm_aa > 0 else 0, 2)
        ating      = round((va / ma_v * 100)           if ma_v  > 0 else 0, 2)
        ating_mrg  = round((mrg / m_mrg * 100)         if m_mrg > 0 else 0, 2)

        rows_out.append({
            "empresa": empresa, "bandeira": bandeira,
            "meta_venda":    round(ma_v, 2),
            "venda":         round(va, 2),
            "desvio":        round(va - ma_v, 2),
            "nro_clientes":  int(nc),
            "prog_cl_ml":    prog_cl,
            "ticket_medio":  tm,
            "prog_tm_ml":    prog_tm,
            "prog_venda_ml": prog_venda,
            "ating":         ating,
            "cresc_cl":      cresc_cl,
            "cresc_tm":      cresc_tm,
            "margem_pdv":    round(mrg, 2),
            "meta_margem":   round(m_mrg, 2),
            "ating_margem":  ating_mrg,
            "quebra":        round(qbr, 2),
            "meta_quebra":   round(m_qbr, 2),
        })

    rows_out.sort(key=lambda x: x["empresa"])

    def _sum(k):    return round(sum(r[k] for r in rows_out), 2)
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
        "empresa": "TOTAIS", "bandeira": "",
        "meta_venda":    tot_ma,
        "venda":         tot_va,
        "desvio":        round(tot_va - tot_ma, 2),
        "nro_clientes":  tot_nc,
        "ticket_medio":  tot_tm,
        "prog_cl_ml":    0,
        "prog_tm_ml":    0,
        "prog_venda_ml": round((tot_va / dias_passados * dias_no_mes / tot_ma * 100) if tot_ma > 0 and dias_passados > 0 else 0, 2),
        "ating":         round((tot_va / tot_ma * 100) if tot_ma > 0 else 0, 2),
        "cresc_cl":      0, "cresc_tm":      0,
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

    # Salva no Firestore e memória
    _fs_set(fs_doc, payload, meta=params)
    _mem_set(mem_key, payload)

    return jsonify(payload)


if __name__ == "__main__":
    print(f"Backend Analise Diaria — http://localhost:5001")
    print(f"Tenant: {TENANT}  |  API Key: {'OK' if API_KEY else 'NAO DEFINIDA'}")
    print(f"Firestore: {'OK' if FIREBASE_OK else 'NAO CONFIGURADO'}")
    app.run(debug=False, host="0.0.0.0", port=5001)
