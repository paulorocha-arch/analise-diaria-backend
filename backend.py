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


# ── Leitura das coleções farol_* do Firestore ────────────────────────────────

def _ler_farol(colecao: str, ano: int, mes: int, dia_ini: int, dia_fim: int) -> list:
    """
    Lê todos os documentos de um período da coleção farol_*.
    Usa batch get (2 round-trips independente do nº de dias).
    """
    if not FIREBASE_OK:
        return []

    # 1) Lê o índice de cada dia para saber quantos lotes existem
    idx_refs = [
        _db.collection(f"{colecao}_index").document(f"{ano}_{mes:02d}_{dia:02d}")
        for dia in range(dia_ini, dia_fim + 1)
    ]
    idx_docs = list(_db.get_all(idx_refs))

    # 2) Monta lista de refs dos documentos de dados
    data_refs = []
    for doc in idx_docs:
        if not doc.exists:
            continue
        n = doc.to_dict().get("total_batches", 1)
        for b in range(n):
            data_refs.append(
                _db.collection(colecao).document(f"{doc.id}_b{b:02d}")
            )

    if not data_refs:
        return []

    # 3) Lê todos os documentos de dados de uma vez
    rows = []
    for doc in _db.get_all(data_refs):
        if doc.exists:
            rows.extend(doc.to_dict().get("rows", []))
    return rows


@app.route("/api/vendas", methods=["POST"])
def api_vendas():
    body          = request.json or {}
    ano           = int(body.get("ano",  datetime.now().year))
    mes           = int(body.get("mes",  datetime.now().month))
    dia_ini       = int(body.get("dia_ini", 1))
    dia_fim       = int(body.get("dia_fim", datetime.now().day - 1))
    empresas      = set(body.get("empresas", []))
    bandeiras     = set(body.get("bandeiras", []))
    compradores_f = set(body.get("compradores", []))
    departamentos = set(body.get("departamentos", []))
    secoes        = set(body.get("secoes", []))

    if dia_ini > dia_fim:
        dia_ini, dia_fim = dia_fim, dia_ini

    from calendar import monthrange
    dias_passados = dia_fim - dia_ini + 1
    dias_no_mes   = monthrange(ano, mes)[1]

    # ── Cache em memória ────────────────────────────────────────────────────
    params  = {
        "ano": ano, "mes": mes, "dia_ini": dia_ini, "dia_fim": dia_fim,
        "empresas": sorted(empresas), "bandeiras": sorted(bandeiras),
        "compradores": sorted(compradores_f),
        "departamentos": sorted(departamentos),
        "secoes": sorted(secoes),
    }
    mem_key = json.dumps(params, sort_keys=True)
    mem = _mem_get(mem_key)
    if mem:
        return jsonify(mem)

    # ── Leitura do Firestore (farol_granular + farol_meta) ──────────────────
    try:
        rows_v = _ler_farol("farol_granular", ano, mes, dia_ini, dia_fim)
        rows_m = _ler_farol("farol_meta",     ano, mes, dia_ini, dia_fim)
        # Meta do mês completo (para Prog. de Venda %)
        rows_m_mes = _ler_farol("farol_meta", ano, mes, 1, dias_no_mes)
    except Exception as e:
        return jsonify({"error": f"Firestore: {e}"}), 500

    if not rows_v:
        return jsonify({"error": "Sem dados no banco para o período selecionado. Execute sync_farol.py para sincronizar."}), 404

    # ── Aplicar filtros sobre os dados granulares ───────────────────────────
    if bandeiras:
        rows_v = [r for r in rows_v if r.get("bandeira") in bandeiras]
        rows_m = [r for r in rows_m if r.get("bandeira") in bandeiras]
    if empresas:
        rows_v = [r for r in rows_v if r.get("empresa") in empresas]
        rows_m = [r for r in rows_m if r.get("empresa") in empresas]
    if compradores_f:
        rows_v = [r for r in rows_v if r.get("comprador") in compradores_f]
    if departamentos:
        rows_v = [r for r in rows_v if r.get("departamento") in departamentos]
    if secoes:
        rows_v = [r for r in rows_v if r.get("secao") in secoes]

    # ── Agregar por empresa ─────────────────────────────────────────────────
    from collections import defaultdict

    agg_v = defaultdict(lambda: {"venda": 0.0, "margem_pdv": 0.0, "nro_clientes": 0, "bandeira": ""})
    for r in rows_v:
        emp = r["empresa"]
        agg_v[emp]["venda"]        += r.get("venda", 0)
        agg_v[emp]["margem_pdv"]   += r.get("margem_pdv", 0)
        agg_v[emp]["nro_clientes"] += r.get("nro_clientes", 0)
        agg_v[emp]["bandeira"]      = r.get("bandeira", "")

    # Meta do período
    agg_m = defaultdict(lambda: {"meta_venda": 0.0, "meta_margem": 0.0})
    for r in rows_m:
        emp = r["empresa"]
        agg_m[emp]["meta_venda"]  += r.get("meta_venda", 0)
        agg_m[emp]["meta_margem"] += r.get("meta_margem", 0)

    # Meta do mês completo (para Prog. de Venda %)
    agg_m_mes = defaultdict(float)
    for r in rows_m_mes:
        if bandeiras and r.get("bandeira") not in bandeiras:
            continue
        if empresas and r.get("empresa") not in empresas:
            continue
        agg_m_mes[r["empresa"]] += r.get("meta_venda", 0)

    # ── Montar linhas de saída ──────────────────────────────────────────────
    rows_out = []
    for emp, v in sorted(agg_v.items()):
        va    = round(v["venda"],      2)
        mrg   = round(v["margem_pdv"], 2)
        nc    = v["nro_clientes"]
        bnd   = v["bandeira"]
        ma_v  = round(agg_m[emp]["meta_venda"],  2)
        m_mrg = round(agg_m[emp]["meta_margem"], 2)
        m_mes = agg_m_mes.get(emp, 0)
        tm    = round(va / nc, 2) if nc > 0 else 0

        ating     = round(va / ma_v * 100,   2) if ma_v  > 0 else 0
        ating_mrg = round(mrg / m_mrg * 100, 2) if m_mrg > 0 else 0
        prog_venda = round(va / dias_passados * dias_no_mes / m_mes * 100, 2) \
                     if m_mes > 0 and dias_passados > 0 else 0

        rows_out.append({
            "empresa":      emp,
            "bandeira":     bnd,
            "meta_venda":   ma_v,
            "venda":        va,
            "desvio":       round(va - ma_v, 2),
            "nro_clientes": nc,
            "prog_cl_ml":   0,
            "ticket_medio": tm,
            "prog_tm_ml":   0,
            "prog_venda_ml": prog_venda,
            "ating":        ating,
            "cresc_cl":     0,
            "cresc_tm":     0,
            "margem_pdv":   mrg,
            "meta_margem":  m_mrg,
            "ating_margem": ating_mrg,
            "quebra":       0,
            "meta_quebra":  0,
        })

    # ── Totais ──────────────────────────────────────────────────────────────
    def _s(k):  return round(sum(r[k] for r in rows_out), 2)
    def _si(k): return sum(r[k] for r in rows_out)

    tot_va   = _s("venda")
    tot_ma   = _s("meta_venda")
    tot_nc   = _si("nro_clientes")
    tot_mrg  = _s("margem_pdv")
    tot_mmrg = _s("meta_margem")
    tot_tm   = round(tot_va / tot_nc, 2) if tot_nc > 0 else 0
    tot_mmes = sum(agg_m_mes.values())

    totais = {
        "venda":         tot_va,
        "meta_venda":    tot_ma,
        "desvio":        round(tot_va - tot_ma, 2),
        "nro_clientes":  tot_nc,
        "ticket_medio":  tot_tm,
        "margem_pdv":    tot_mrg,
        "meta_margem":   tot_mmrg,
        "quebra":        0,
        "meta_quebra":   0,
        "ating":         round(tot_va / tot_ma * 100, 2) if tot_ma > 0 else 0,
        "ating_margem":  round(tot_mrg / tot_mmrg * 100, 2) if tot_mmrg > 0 else 0,
        "prog_venda_ml": round(tot_va / dias_passados * dias_no_mes / tot_mmes * 100, 2)
                         if tot_mmes > 0 and dias_passados > 0 else 0,
        "cresc_cl": 0, "cresc_tm": 0,
    }

    payload = {
        "fonte": "firestore",
        "filtros": {"ano": ano, "mes": mes, "dia_ini": dia_ini, "dia_fim": dia_fim,
                    "dias_passados": dias_passados, "dias_no_mes": dias_no_mes},
        "totais": totais,
        "data":   rows_out,
    }

    _mem_set(mem_key, payload)
    return jsonify(payload)


if __name__ == "__main__":
    print(f"Backend Analise Diaria — http://localhost:5001")
    print(f"Tenant: {TENANT}  |  API Key: {'OK' if API_KEY else 'NAO DEFINIDA'}")
    print(f"Firestore: {'OK' if FIREBASE_OK else 'NAO CONFIGURADO'}")
    app.run(debug=False, host="0.0.0.0", port=5001)
