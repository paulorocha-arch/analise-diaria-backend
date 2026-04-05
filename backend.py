#!/usr/bin/env python3
"""
Análise Diária — Backend
Arquitetura: Qlik Cloud → Firestore (cache) → Dashboard
Iniciar: python backend.py
"""

import asyncio, base64, hashlib, json, os, sys, time
from datetime import datetime, timezone
from flask import Flask, jsonify, request, send_from_directory
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
    """Hipercubo simples sem paginação. Medidas retornam como _m0, _m1, ..."""
    q_dims    = [{"qDef": {"qFieldDefs": [d]}} for d in dimensions]
    q_meas    = [{"qDef": {"qDef": m}} for m in measures]
    n_cols    = len(dimensions) + len(measures)
    col_names = dimensions + [f"_m{i}" for i in range(len(measures))]

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
        msg = await recv_until(lid, timeout=45)
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


async def _buscar_vendas_qlik(ano, mes, dia_ini, dia_fim, extra_v, emp_extra):
    """Busca vendas + meta direto no Qlik, com filtros embutidos no set expression.
    Retorna (rows_v, rows_m) onde cada row é um dict com chaves nomeadas.
    """
    from calendar import monthrange
    ma   = _mes_ano(ano, mes)
    days = _days(dia_ini, dia_fim)
    dias_no_mes = monthrange(ano, mes)[1]

    set_v     = f"{{<FlagFatosVendas={{1}},[Mês/Ano]={{'{ma}'}},Dia={{{days}}}{extra_v}{emp_extra}}}"
    set_m     = f"{{<FlagFatosMetas={{1}},[Mês/Ano]={{'{ma}'}},Dia={{{days}}}{emp_extra}}}"
    set_m_mes = f"{{<FlagFatosMetas={{1}},[Mês/Ano]={{'{ma}'}}{emp_extra}}}"

    meas_v = [
        f"Sum({set_v} #Medida1)",
        f"Sum({set_v} #Medida2)",
        f"Count({set_v} Distinct ChaveNF)",
    ]
    meas_m = [
        f"Sum({set_m} #Medida1)",
        f"Sum({set_m} #Medida2)",
        f"Sum({set_m_mes} #Medida1)",
    ]

    async def _run(rpc, recv_until, doc_handle):
        def make_obj(dims, meas_exprs, n_rows=500):
            q_d = [{"qDef": {"qFieldDefs": [d]}} for d in dims]
            q_m = [{"qDef": {"qDef": m}} for m in meas_exprs]
            nc  = len(dims) + len(meas_exprs)
            return {
                "qInfo": {"qType": "HyperCube"},
                "qHyperCubeDef": {
                    "qDimensions": q_d, "qMeasures": q_m,
                    "qInitialDataFetch": [{"qTop": 0, "qLeft": 0,
                                           "qHeight": n_rows, "qWidth": nc}],
                    "qMode": "S",
                    "qSuppressZero": False, "qSuppressMissing": False,
                },
            }

        def parse(result_msg, dims, meas_keys):
            col_names = dims + meas_keys
            hc    = result_msg.get("result", {}).get("qLayout", {}).get("qHyperCube", {})
            pages = hc.get("qDataPages", [])
            rows  = []
            for page in pages:
                for row in page.get("qMatrix", []):
                    r = {}
                    for i, cell in enumerate(row):
                        if i < len(col_names):
                            r[col_names[i]] = cell.get("qText",
                                                        str(cell.get("qNum", "")))
                    rows.append(r)
            return rows

        # ── Vendas ──
        cid_v = await rpc("CreateSessionObject", [make_obj(["Empresa"], meas_v)], doc_handle)
        msg   = await recv_until(cid_v)
        if not msg:
            raise RuntimeError("Timeout criar hipercubo vendas")
        hv = msg.get("result", {}).get("qReturn", {}).get("qHandle")

        lid_v = await rpc("GetLayout", [], hv)
        lmsg_v = await recv_until(lid_v, timeout=60)
        if not lmsg_v:
            raise RuntimeError("Timeout layout vendas")

        # ── Meta ──
        cid_m = await rpc("CreateSessionObject", [make_obj(["Empresa"], meas_m)], doc_handle)
        msg   = await recv_until(cid_m)
        if not msg:
            raise RuntimeError("Timeout criar hipercubo meta")
        hm = msg.get("result", {}).get("qReturn", {}).get("qHandle")

        lid_m = await rpc("GetLayout", [], hm)
        lmsg_m = await recv_until(lid_m, timeout=60)
        if not lmsg_m:
            raise RuntimeError("Timeout layout meta")

        rv = parse(lmsg_v, ["Empresa"], ["venda", "margem_pdv", "nro_clientes"])
        rm = parse(lmsg_m, ["Empresa"], ["meta_venda", "meta_margem", "meta_venda_mes"])
        return rv, rm

    return await _engine_session(APP_FAROL, _run)


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
    routes = sorted(r.rule for r in app.url_map.iter_rules())
    return jsonify({"status": "ok", "tenant": TENANT,
                    "api_key": bool(API_KEY), "firestore": FIREBASE_OK,
                    "routes": routes})


@app.route("/api/drill", methods=["POST"])
def api_drill():
    """
    Drill-down: Departamento > Seção para uma empresa específica.
    Consulta Qlik diretamente com filtros via set expression.
    """
    body          = request.json or {}
    empresa       = body.get("empresa", "")
    ano           = int(body.get("ano",  datetime.now().year))
    mes           = int(body.get("mes",  datetime.now().month))
    dia_ini       = int(body.get("dia_ini", 1))
    dia_fim       = int(body.get("dia_fim", datetime.now().day - 1))
    compradores_f = body.get("compradores",   [])
    departamentos = body.get("departamentos", [])
    secoes_f      = body.get("secoes",        [])

    if not empresa:
        return jsonify({"error": "empresa obrigatória"}), 400
    if dia_ini > dia_fim:
        dia_ini, dia_fim = dia_fim, dia_ini

    mem_key = json.dumps({
        "drill": True, "empresa": empresa,
        "ano": ano, "mes": mes, "dia_ini": dia_ini, "dia_fim": dia_fim,
        "compradores": sorted(compradores_f),
        "departamentos": sorted(departamentos), "secoes": sorted(secoes_f),
    }, sort_keys=True)
    mem = _mem_get(mem_key)
    if mem:
        return jsonify(mem)

    # ── Qlik set expression ──────────────────────────────────────────────────
    ma         = _mes_ano(ano, mes)
    days       = _days(dia_ini, dia_fim)
    extra      = _set_extra(compradores_f, departamentos, secoes_f)
    emp_filter = f",Empresa={{'{empresa}'}}"
    set_v = (f"{{<FlagFatosVendas={{1}},[Mês/Ano]={{'{ma}'}},"
             f"Dia={{{days}}}{extra}{emp_filter}}}")

    try:
        rows = _run_async(_hypercube(
            APP_FAROL,
            ["Nível 1", "Nível 2"],
            [
                f"Sum({set_v} #Medida1)",
                f"Sum({set_v} #Medida2)",
                f"Count({set_v} Distinct ChaveNF)",
            ],
            rows=500,
        ))
    except Exception as e:
        return jsonify({"error": f"Qlik: {e}"}), 500

    # ── Agrupar por Departamento > Seção ─────────────────────────────────────
    from collections import defaultdict
    deps = defaultdict(list)
    for r in rows:
        dept  = (r.get("Nível 1") or "").strip()
        secao = (r.get("Nível 2") or "").strip()
        if not dept:
            continue
        deps[dept].append({
            "secao":        secao,
            "venda":        round(_float(r.get("_m0", "0")), 2),
            "margem_pdv":   round(_float(r.get("_m1", "0")), 2),
            "nro_clientes": int(_float(r.get("_m2", "0"))),
        })

    result = []
    for dept in sorted(deps):
        slist = sorted(deps[dept], key=lambda x: x["secao"])
        result.append({
            "departamento": dept,
            "venda":        round(sum(s["venda"]        for s in slist), 2),
            "margem_pdv":   round(sum(s["margem_pdv"]   for s in slist), 2),
            "nro_clientes": sum(s["nro_clientes"]        for s in slist),
            "secoes":       slist,
        })

    payload = {"empresa": empresa, "data": result}
    _mem_set(mem_key, payload)
    return jsonify(payload)


@app.route("/api/filtros")
def api_filtros():
    """Retorna listas de filtros diretamente do Qlik (valores distintos por campo)."""
    mem = _mem_get("__filtros__")
    if mem:
        return jsonify(mem)

    try:
        rows_emp, rows_dep, rows_sec, rows_com = _run_async_all(
            _hypercube(APP_FAROL, ["Empresa"],   [], rows=200),
            _hypercube(APP_FAROL, ["Nível 1"],   [], rows=100),
            _hypercube(APP_FAROL, ["Nível 2"],   [], rows=500),
            _hypercube(APP_FAROL, ["Comprador"], [], rows=500),
        )

        empresas      = sorted(r["Empresa"]   for r in rows_emp
                               if r.get("Empresa")   and r["Empresa"]   not in ("-", ""))
        departamentos = sorted(r["Nível 1"]   for r in rows_dep
                               if r.get("Nível 1")   and r["Nível 1"]   not in ("-", ""))
        secoes        = sorted(r["Nível 2"]   for r in rows_sec
                               if r.get("Nível 2")   and r["Nível 2"]   not in ("-", ""))
        compradores   = sorted(r["Comprador"] for r in rows_com
                               if r.get("Comprador") and r["Comprador"]
                               not in ("-", "", "NÃO IDENTIFICADO"))

        def _bandeira(empresa):
            return "BIG BOX" if empresa.upper().startswith("BIG") else "ULT"
        bandeiras = sorted({_bandeira(e) for e in empresas})

        payload = {
            "empresas": empresas, "bandeiras": bandeiras,
            "compradores": compradores,
            "departamentos": departamentos,
            "secoes": secoes,
        }
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
    """
    Consulta vendas e metas diretamente no Qlik com os filtros embutidos
    nos set expressions. Listas vazias = sem filtro (todos os valores).
    """
    body          = request.json or {}
    ano           = int(body.get("ano",  datetime.now().year))
    mes           = int(body.get("mes",  datetime.now().month))
    dia_ini       = int(body.get("dia_ini", 1))
    dia_fim       = int(body.get("dia_fim", datetime.now().day - 1))
    empresas_f    = body.get("empresas",      [])   # [] = todos
    bandeiras_f   = body.get("bandeiras",     [])   # [] = todos
    compradores_f = body.get("compradores",   [])   # [] = todos
    departamentos = body.get("departamentos", [])   # [] = todos
    secoes        = body.get("secoes",        [])   # [] = todos

    if dia_ini > dia_fim:
        dia_ini, dia_fim = dia_fim, dia_ini

    from calendar import monthrange
    dias_passados = dia_fim - dia_ini + 1
    dias_no_mes   = monthrange(ano, mes)[1]

    # ── Cache ───────────────────────────────────────────────────────────────
    params  = {
        "ano": ano, "mes": mes, "dia_ini": dia_ini, "dia_fim": dia_fim,
        "empresas": sorted(empresas_f), "bandeiras": sorted(bandeiras_f),
        "compradores": sorted(compradores_f),
        "departamentos": sorted(departamentos),
        "secoes": sorted(secoes),
    }
    mem_key = json.dumps(params, sort_keys=True)
    mem = _mem_get(mem_key)
    if mem:
        return jsonify(mem)

    # ── Build set extensions ─────────────────────────────────────────────────
    # Filtros de produto/comprador (aplicam só a vendas)
    extra_v = _set_extra(compradores_f, departamentos, secoes)

    # Filtros de empresa/bandeira (aplicam a vendas e meta)
    emp_parts = []
    if empresas_f:
        vals = ",".join("'" + v + "'" for v in empresas_f)
        emp_parts.append(f"Empresa={{{vals}}}")
    if bandeiras_f:
        vals = ",".join("'" + v + "'" for v in bandeiras_f)
        emp_parts.append(f"Bandeira={{{vals}}}")
    emp_extra = ("," + ",".join(emp_parts)) if emp_parts else ""

    # ── Consulta Qlik ────────────────────────────────────────────────────────
    try:
        rows_v, rows_m = _run_async(
            _buscar_vendas_qlik(ano, mes, dia_ini, dia_fim, extra_v, emp_extra)
        )
    except Exception as e:
        return jsonify({"error": f"Qlik: {e}"}), 500

    if not rows_v:
        return jsonify({"error": "Sem dados no Qlik para o período selecionado."}), 404

    # ── Montar mapa de meta por empresa ─────────────────────────────────────
    agg_m = {}
    for r in rows_m:
        emp = r.get("Empresa", "").strip()
        if not emp:
            continue
        agg_m[emp] = {
            "meta_venda":     _float(r.get("meta_venda",     "0")),
            "meta_margem":    _float(r.get("meta_margem",    "0")),
            "meta_venda_mes": _float(r.get("meta_venda_mes", "0")),
        }

    # ── Montar linhas de saída ───────────────────────────────────────────────
    def _bandeira(empresa):
        return "BIG BOX" if empresa.upper().startswith("BIG") else "ULT"

    rows_out = []
    for r in rows_v:
        emp = r.get("Empresa", "").strip()
        if not emp:
            continue
        va    = _float(r.get("venda",        "0"))
        mrg   = _float(r.get("margem_pdv",   "0"))
        nc    = int(_float(r.get("nro_clientes", "0")))
        bnd   = _bandeira(emp)

        m     = agg_m.get(emp, {})
        ma_v  = m.get("meta_venda",     0)
        m_mrg = m.get("meta_margem",    0)
        m_mes = m.get("meta_venda_mes", 0)
        tm    = round(va / nc, 2) if nc > 0 else 0

        ating     = round(va / ma_v  * 100, 2) if ma_v  > 0 else 0
        ating_mrg = round(mrg / m_mrg * 100, 2) if m_mrg > 0 else 0
        prog_venda = round(va / dias_passados * dias_no_mes / m_mes * 100, 2) \
                     if m_mes > 0 and dias_passados > 0 else 0

        rows_out.append({
            "empresa":       emp,
            "bandeira":      bnd,
            "meta_venda":    round(ma_v,  2),
            "venda":         round(va,    2),
            "desvio":        round(va - ma_v, 2),
            "nro_clientes":  nc,
            "prog_cl_ml":    0,
            "ticket_medio":  tm,
            "prog_tm_ml":    0,
            "prog_venda_ml": prog_venda,
            "ating":         ating,
            "cresc_cl":      0,
            "cresc_tm":      0,
            "margem_pdv":    round(mrg,  2),
            "meta_margem":   round(m_mrg, 2),
            "ating_margem":  ating_mrg,
            "quebra":        0,
            "meta_quebra":   0,
        })

    rows_out.sort(key=lambda r: r["empresa"])

    # ── Totais ───────────────────────────────────────────────────────────────
    def _s(k):  return round(sum(r[k] for r in rows_out), 2)
    def _si(k): return sum(r[k] for r in rows_out)

    tot_va   = _s("venda")
    tot_ma   = _s("meta_venda")
    tot_nc   = _si("nro_clientes")
    tot_mrg  = _s("margem_pdv")
    tot_mmrg = _s("meta_margem")
    tot_tm   = round(tot_va / tot_nc, 2) if tot_nc > 0 else 0
    tot_mmes = sum(m.get("meta_venda_mes", 0) for m in agg_m.values())

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
        "ating":         round(tot_va / tot_ma  * 100, 2) if tot_ma  > 0 else 0,
        "ating_margem":  round(tot_mrg / tot_mmrg * 100, 2) if tot_mmrg > 0 else 0,
        "prog_venda_ml": round(tot_va / dias_passados * dias_no_mes / tot_mmes * 100, 2)
                         if tot_mmes > 0 and dias_passados > 0 else 0,
        "cresc_cl": 0, "cresc_tm": 0,
    }

    payload = {
        "fonte": "qlik",
        "filtros": {"ano": ano, "mes": mes, "dia_ini": dia_ini, "dia_fim": dia_fim,
                    "dias_passados": dias_passados, "dias_no_mes": dias_no_mes},
        "totais": totais,
        "data":   rows_out,
    }

    _mem_set(mem_key, payload)
    return jsonify(payload)


@app.route("/api/vendas/drill", methods=["POST"])
def api_vendas_drill():
    """
    Retorna detalhamento de uma loja por Departamento > Seção.
    Lê do Firestore (farol_granular) — muito mais rápido que Qlik para drill.
    Parâmetros: empresa, ano, mes, dia_ini, dia_fim, compradores, departamentos, secoes.
    """
    body          = request.json or {}
    empresa       = body.get("empresa", "")
    ano           = int(body.get("ano",  datetime.now().year))
    mes           = int(body.get("mes",  datetime.now().month))
    dia_ini       = int(body.get("dia_ini", 1))
    dia_fim       = int(body.get("dia_fim", datetime.now().day - 1))
    compradores_f = set(body.get("compradores",   []))
    departamentos = set(body.get("departamentos", []))
    secoes_f      = set(body.get("secoes",        []))

    if not empresa:
        return jsonify({"error": "empresa obrigatória"}), 400
    if dia_ini > dia_fim:
        dia_ini, dia_fim = dia_fim, dia_ini

    mem_key = json.dumps({
        "drill": True, "empresa": empresa,
        "ano": ano, "mes": mes, "dia_ini": dia_ini, "dia_fim": dia_fim,
        "compradores": sorted(compradores_f),
        "departamentos": sorted(departamentos), "secoes": sorted(secoes_f),
    }, sort_keys=True)
    mem = _mem_get(mem_key)
    if mem:
        return jsonify(mem)

    # ── Leitura do Firestore ─────────────────────────────────────────────────
    try:
        rows = _ler_farol("farol_granular", ano, mes, dia_ini, dia_fim)
    except Exception as e:
        return jsonify({"error": f"Firestore: {e}"}), 500

    if not rows:
        return jsonify({"empresa": empresa, "data": []})

    # ── Filtros ──────────────────────────────────────────────────────────────
    rows = [r for r in rows if r.get("empresa") == empresa]
    if compradores_f:
        rows = [r for r in rows if r.get("comprador") in compradores_f]
    if departamentos:
        rows = [r for r in rows if r.get("departamento") in departamentos]
    if secoes_f:
        rows = [r for r in rows if r.get("secao") in secoes_f]

    # ── Agrupar por Departamento > Seção ─────────────────────────────────────
    from collections import defaultdict
    deps = defaultdict(lambda: defaultdict(
        lambda: {"venda": 0.0, "margem_pdv": 0.0, "nro_clientes": 0}
    ))
    for r in rows:
        dept  = (r.get("departamento") or "").strip()
        secao = (r.get("secao")        or "").strip()
        if not dept:
            continue
        deps[dept][secao]["venda"]        += r.get("venda",        0)
        deps[dept][secao]["margem_pdv"]   += r.get("margem_pdv",   0)
        deps[dept][secao]["nro_clientes"] += r.get("nro_clientes", 0)

    result = []
    for dept in sorted(deps):
        secoes_list = []
        for secao in sorted(deps[dept]):
            s = deps[dept][secao]
            secoes_list.append({
                "secao":        secao,
                "venda":        round(s["venda"],      2),
                "margem_pdv":   round(s["margem_pdv"], 2),
                "nro_clientes": s["nro_clientes"],
            })
        result.append({
            "departamento": dept,
            "venda":        round(sum(s["venda"]        for s in secoes_list), 2),
            "margem_pdv":   round(sum(s["margem_pdv"]   for s in secoes_list), 2),
            "nro_clientes": sum(s["nro_clientes"]        for s in secoes_list),
            "secoes":       secoes_list,
        })

    payload = {"empresa": empresa, "data": result}
    _mem_set(mem_key, payload)
    return jsonify(payload)


@app.route("/")
@app.route("/<path:filename>")
def serve_static(filename="index.html"):
    """Serve arquivos estáticos para desenvolvimento local."""
    base = os.path.dirname(os.path.abspath(__file__))
    return send_from_directory(base, filename)


if __name__ == "__main__":
    print(f"Backend Analise Diaria — http://localhost:5001")
    print(f"Tenant: {TENANT}  |  API Key: {'OK' if API_KEY else 'NAO DEFINIDA'}")
    print(f"Firestore: {'OK' if FIREBASE_OK else 'NAO CONFIGURADO'}")
    app.run(debug=False, host="0.0.0.0", port=5001)
