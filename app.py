"""
Oficina Jurídica Enriquez Flores & Asociados — Flask Web App
Base de datos: PostgreSQL (Railway) con fallback a SQLite para desarrollo local
"""
from flask import Flask, render_template, request, redirect, url_for, session, jsonify, flash, send_file
import os, datetime, re, hashlib, io
from pathlib import Path
from functools import wraps

app = Flask(__name__)

# SECRET_KEY debe estar definida como variable de entorno en Render.
# Si no está definida, se usa un valor fijo como fallback (no recomendado en producción).
_secret = os.environ.get("SECRET_KEY", "oj_enriquez_flores_2024_secret_key_cambiar_PROD")
if not _secret or _secret.strip() == "":
    raise RuntimeError("SECRET_KEY no está configurada en las variables de entorno.")
app.secret_key = _secret

# Configuración de sesiones para que funcionen correctamente con múltiples workers
# y con redirects HTTPS en Render
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
# En Render siempre es HTTPS, así que activar Secure es seguro
app.config["SESSION_COOKIE_SECURE"] = os.environ.get("RENDER", "") != ""

# ── Detectar si usamos PostgreSQL o SQLite ──────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "")
USE_POSTGRES = DATABASE_URL.startswith("postgres")

if USE_POSTGRES:
    import psycopg2
    import psycopg2.extras
    # Railway usa "postgres://" pero psycopg2 necesita "postgresql://"
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
else:
    import sqlite3
    APP_DIR    = Path(os.environ.get("APP_DIR", str(Path.home() / "OJEnriquezFlores")))
    DB_PATH    = APP_DIR / "oficina_juridica.db"
    BACKUP_DIR = APP_DIR / "respaldos"
    APP_DIR.mkdir(exist_ok=True)
    BACKUP_DIR.mkdir(exist_ok=True)

ESTADOS_E = ["Ingresado","Redactada","Firmada","En Proceso","Listo para Retirar","Entregado al Cliente"]
TIPOS_E   = ["Compraventa","Traspaso de Vehículo","Constitución de Empresa","Mandato",
             "Hipoteca","Donación","Protocolización","Auténtica","Carta Poder","Testimonio","Otro"]
CONC_ING  = ["Traspaso de Vehículo","Auténtica de Firma","Carta Poder","Compraventa",
             "Mandato","Constitución de Empresa","Protocolización","Hipoteca","Testimonio","Consulta","Otro"]
CONC_GAS  = ["Papelería","Viáticos","Procurador","Aranceles Registro","Servicios Básicos",
             "Combustible","Honorarios Externos","Limpieza","Alimentación","Mantenimiento","Otro"]
OCUP      = ["Particular","Empresario","Empleado","Profesional","Comerciante","Agricultor","Otro"]
RANGOS    = ["Hoy","Esta semana","Quincena","Este mes","Rango personalizado"]

# ── DB ──────────────────────────────────────────────────────────────
def get_db():
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        return conn
    else:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        return conn

def dbq(sql, params=(), one=False):
    """Ejecuta SELECT y devuelve lista de dicts o un dict."""
    # Convertir placeholders ? a %s para PostgreSQL
    if USE_POSTGRES:
        sql = sql.replace("?", "%s")
    conn = get_db()
    try:
        if USE_POSTGRES:
            cur = conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            rows = [dict(r) for r in rows]
        else:
            rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
        return (rows[0] if rows else None) if one else rows
    finally:
        conn.close()

def dbx(sql, params=()):
    """Ejecuta INSERT/UPDATE/DELETE y devuelve el id del último registro."""
    if USE_POSTGRES:
        sql = sql.replace("?", "%s")
        # Para INSERT, agregar RETURNING id si no lo tiene
        if sql.strip().upper().startswith("INSERT") and "RETURNING" not in sql.upper():
            sql = sql.rstrip().rstrip(";") + " RETURNING id"
    conn = get_db()
    try:
        if USE_POSTGRES:
            cur = conn.cursor()
            cur.execute(sql, params)
            conn.commit()
            if sql.strip().upper().startswith("INSERT") and "RETURNING" in sql.upper():
                row = cur.fetchone()
                return row["id"] if row else None
            return None
        else:
            cur = conn.execute(sql, params)
            conn.commit()
            return cur.lastrowid
    finally:
        conn.close()

def fmtQ(n):
    try: return f"Q {float(n):,.2f}"
    except: return "Q 0.00"

def rango_sql(rango, desde=None, hasta=None):
    hoy = datetime.date.today()
    if USE_POSTGRES:
        if rango == "Hoy": return f"fecha = '{hoy}'"
        if rango == "Esta semana":
            return f"fecha >= '{hoy - datetime.timedelta(days=hoy.weekday())}'"
        if rango == "Este mes":
            return f"TO_CHAR(fecha::date, 'YYYY-MM') = '{hoy.strftime('%Y-%m')}'"
        if rango == "Quincena":
            import calendar
            if hoy.day <= 15: ini,fin = hoy.replace(day=1), hoy.replace(day=15)
            else: ini,fin = hoy.replace(day=16), hoy.replace(day=calendar.monthrange(hoy.year,hoy.month)[1])
            return f"fecha BETWEEN '{ini}' AND '{fin}'"
        if rango == "Rango personalizado" and desde and hasta:
            return f"fecha BETWEEN '{desde}' AND '{hasta}'"
    else:
        if rango == "Hoy": return f"fecha = '{hoy}'"
        if rango == "Esta semana":
            return f"fecha >= '{hoy - datetime.timedelta(days=hoy.weekday())}'"
        if rango == "Este mes":
            return f"strftime('%Y-%m', fecha) = '{hoy.strftime('%Y-%m')}'"
        if rango == "Quincena":
            import calendar
            if hoy.day <= 15: ini,fin = hoy.replace(day=1), hoy.replace(day=15)
            else: ini,fin = hoy.replace(day=16), hoy.replace(day=calendar.monthrange(hoy.year,hoy.month)[1])
            return f"fecha BETWEEN '{ini}' AND '{fin}'"
        if rango == "Rango personalizado" and desde and hasta:
            return f"fecha BETWEEN '{desde}' AND '{hasta}'"
    return "1=1"

def init_db():
    conn = get_db()
    try:
        cur = conn.cursor() if USE_POSTGRES else conn

        if USE_POSTGRES:
            schema = """
            CREATE TABLE IF NOT EXISTS escrituras (
                id SERIAL PRIMARY KEY, numero TEXT NOT NULL UNIQUE,
                tipo TEXT NOT NULL, cliente TEXT NOT NULL, descripcion TEXT DEFAULT '',
                fecha TEXT NOT NULL, estado TEXT NOT NULL DEFAULT 'Ingresado',
                honorarios REAL NOT NULL DEFAULT 0, notas TEXT DEFAULT '',
                fecha_reg_mercantil TEXT DEFAULT '', hora_reg_mercantil TEXT DEFAULT '',
                avance_reg TEXT DEFAULT '', porcentaje_avance INTEGER NOT NULL DEFAULT 0,
                num_documento TEXT DEFAULT '',
                created_at TEXT DEFAULT to_char(now(),'YYYY-MM-DD HH24:MI:SS'),
                updated_at TEXT DEFAULT to_char(now(),'YYYY-MM-DD HH24:MI:SS')
            );
            CREATE TABLE IF NOT EXISTS ingresos (
                id SERIAL PRIMARY KEY, fecha TEXT NOT NULL,
                concepto TEXT NOT NULL, cliente TEXT DEFAULT '',
                monto REAL NOT NULL, nota TEXT DEFAULT '',
                created_at TEXT DEFAULT to_char(now(),'YYYY-MM-DD HH24:MI:SS')
            );
            CREATE TABLE IF NOT EXISTS gastos (
                id SERIAL PRIMARY KEY, fecha TEXT NOT NULL,
                concepto TEXT NOT NULL, descripcion TEXT DEFAULT '',
                monto REAL NOT NULL, nota TEXT DEFAULT '',
                created_at TEXT DEFAULT to_char(now(),'YYYY-MM-DD HH24:MI:SS')
            );
            CREATE TABLE IF NOT EXISTS respaldos (
                id SERIAL PRIMARY KEY, nombre TEXT NOT NULL,
                ruta TEXT NOT NULL, tamanio_kb REAL,
                created_at TEXT DEFAULT to_char(now(),'YYYY-MM-DD HH24:MI:SS')
            );
            CREATE TABLE IF NOT EXISTS usuarios (
                id SERIAL PRIMARY KEY, username TEXT NOT NULL UNIQUE,
                password TEXT NOT NULL, rol TEXT NOT NULL DEFAULT 'usuario',
                activo INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT to_char(now(),'YYYY-MM-DD HH24:MI:SS')
            );
            CREATE TABLE IF NOT EXISTS clientes (
                id SERIAL PRIMARY KEY, nombre TEXT NOT NULL,
                telefono TEXT DEFAULT '', whatsapp TEXT DEFAULT '',
                email TEXT DEFAULT '', dpi TEXT DEFAULT '',
                direccion TEXT DEFAULT '', ocupacion TEXT DEFAULT '',
                notas TEXT DEFAULT '', activo INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT to_char(now(),'YYYY-MM-DD HH24:MI:SS'),
                updated_at TEXT DEFAULT to_char(now(),'YYYY-MM-DD HH24:MI:SS')
            );
            CREATE INDEX IF NOT EXISTS idx_e_fecha ON escrituras(fecha);
            CREATE INDEX IF NOT EXISTS idx_e_estado ON escrituras(estado);
            CREATE INDEX IF NOT EXISTS idx_e_cliente ON escrituras(cliente);
            CREATE INDEX IF NOT EXISTS idx_i_fecha ON ingresos(fecha);
            CREATE INDEX IF NOT EXISTS idx_g_fecha ON gastos(fecha);
            CREATE INDEX IF NOT EXISTS idx_c_nombre ON clientes(nombre);
            """
            for s in schema.strip().split(";"):
                s = s.strip()
                if s:
                    try: cur.execute(s)
                    except: conn.rollback()
        else:
            schema = """
            CREATE TABLE IF NOT EXISTS escrituras (
                id INTEGER PRIMARY KEY AUTOINCREMENT, numero TEXT NOT NULL UNIQUE,
                tipo TEXT NOT NULL, cliente TEXT NOT NULL, descripcion TEXT DEFAULT '',
                fecha TEXT NOT NULL, estado TEXT NOT NULL DEFAULT 'Ingresado',
                honorarios REAL NOT NULL DEFAULT 0, notas TEXT DEFAULT '',
                fecha_reg_mercantil TEXT DEFAULT '', hora_reg_mercantil TEXT DEFAULT '',
                avance_reg TEXT DEFAULT '', porcentaje_avance INTEGER NOT NULL DEFAULT 0,
                num_documento TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now','localtime')),
                updated_at TEXT DEFAULT (datetime('now','localtime'))
            );
            CREATE TABLE IF NOT EXISTS ingresos (
                id INTEGER PRIMARY KEY AUTOINCREMENT, fecha TEXT NOT NULL,
                concepto TEXT NOT NULL, cliente TEXT DEFAULT '',
                monto REAL NOT NULL, nota TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now','localtime'))
            );
            CREATE TABLE IF NOT EXISTS gastos (
                id INTEGER PRIMARY KEY AUTOINCREMENT, fecha TEXT NOT NULL,
                concepto TEXT NOT NULL, descripcion TEXT DEFAULT '',
                monto REAL NOT NULL, nota TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now','localtime'))
            );
            CREATE TABLE IF NOT EXISTS respaldos (
                id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL,
                ruta TEXT NOT NULL, tamanio_kb REAL,
                created_at TEXT DEFAULT (datetime('now','localtime'))
            );
            CREATE TABLE IF NOT EXISTS usuarios (
                id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL UNIQUE,
                password TEXT NOT NULL, rol TEXT NOT NULL DEFAULT 'usuario',
                activo INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now','localtime'))
            );
            CREATE TABLE IF NOT EXISTS clientes (
                id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL,
                telefono TEXT DEFAULT '', whatsapp TEXT DEFAULT '',
                email TEXT DEFAULT '', dpi TEXT DEFAULT '',
                direccion TEXT DEFAULT '', ocupacion TEXT DEFAULT '',
                notas TEXT DEFAULT '', activo INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now','localtime')),
                updated_at TEXT DEFAULT (datetime('now','localtime'))
            );
            CREATE INDEX IF NOT EXISTS idx_e_fecha ON escrituras(fecha);
            CREATE INDEX IF NOT EXISTS idx_e_estado ON escrituras(estado);
            CREATE INDEX IF NOT EXISTS idx_e_cliente ON escrituras(cliente);
            CREATE INDEX IF NOT EXISTS idx_i_fecha ON ingresos(fecha);
            CREATE INDEX IF NOT EXISTS idx_g_fecha ON gastos(fecha);
            CREATE INDEX IF NOT EXISTS idx_c_nombre ON clientes(nombre);
            """
            for s in schema.strip().split(";"):
                s = s.strip()
                if s:
                    try: conn.execute(s)
                    except: pass
            # migrate legacy columns
            existing = [r[1] for r in conn.execute("PRAGMA table_info(escrituras)").fetchall()]
            for col,defn in [("num_documento","TEXT DEFAULT ''"),("porcentaje_avance","INTEGER DEFAULT 0"),
                             ("avance_reg","TEXT DEFAULT ''"),("hora_reg_mercantil","TEXT DEFAULT ''"),
                             ("fecha_reg_mercantil","TEXT DEFAULT ''")]:
                if col not in existing:
                    try: conn.execute(f"ALTER TABLE escrituras ADD COLUMN {col} {defn}")
                    except: pass

        conn.commit()
    finally:
        conn.close()

    # seed usuarios iniciales
    def h(p): return hashlib.sha256(p.encode()).hexdigest()
    for u,p,r in [('administrador',h('admin123'),'administrador'),
                  ('usuario1',h('usuario1'),'usuario'),('usuario2',h('usuario2'),'usuario')]:
        try:
            if USE_POSTGRES:
                dbx('INSERT INTO usuarios(username,password,rol) VALUES(%s,%s,%s) ON CONFLICT (username) DO NOTHING',(u,p,r))
            else:
                dbx('INSERT OR IGNORE INTO usuarios(username,password,rol) VALUES(?,?,?)',(u,p,r))
        except: pass

# ── Auth ─────────────────────────────────────────────────────────────
def login_required(f):
    @wraps(f)
    def d(*a,**k):
        if "user" not in session: return redirect(url_for("login"))
        return f(*a,**k)
    return d

def admin_required(f):
    @wraps(f)
    def d(*a,**k):
        if "user" not in session: return redirect(url_for("login"))
        if session["user"]["rol"] != "administrador":
            flash("Acceso restringido al administrador.","error")
            return redirect(url_for("dashboard"))
        return f(*a,**k)
    return d

@app.route("/login", methods=["GET","POST"])
def login():
    error = None
    if request.method == "POST":
        u = request.form.get("username","").strip()
        p = hashlib.sha256(request.form.get("password","").encode()).hexdigest()
        user = dbq("SELECT * FROM usuarios WHERE username=? AND activo=1",(u,),one=True)
        if user and user["password"] == p:
            session.permanent = True
            session["user"] = {"id":user["id"],"username":user["username"],"rol":user["rol"]}
            return redirect(url_for("dashboard"))
        error = "Usuario o contraseña incorrectos"
    return render_template("login.html", error=error)

@app.route("/logout")
def logout():
    session.clear(); return redirect(url_for("login"))

# ── Dashboard ────────────────────────────────────────────────────────
@app.route("/")
@login_required
def dashboard():
    conteo = {}
    for e in ESTADOS_E:
        r = dbq("SELECT COUNT(*) as n FROM escrituras WHERE estado=?",(e,),one=True)
        conteo[e] = r["n"] if r else 0
    conteo["Total"] = sum(conteo.values())
    hoy = datetime.date.today().isoformat()
    r = dbq("SELECT COUNT(*) as n FROM escrituras WHERE fecha=?",(hoy,),one=True)
    conteo["Ingresadas Hoy"] = r["n"] if r else 0
    limite = (datetime.date.today()-datetime.timedelta(days=10)).isoformat()
    alertas = dbq("""SELECT * FROM escrituras WHERE fecha_reg_mercantil!=''
        AND fecha_reg_mercantil<=? AND estado NOT IN ('Listo para Retirar','Entregado al Cliente')
        ORDER BY fecha_reg_mercantil ASC""",(limite,))
    recientes = dbq("SELECT * FROM escrituras ORDER BY id DESC LIMIT 8")
    finanzas = None
    if session["user"]["rol"]=="administrador":
        w = rango_sql("Este mes")
        i = dbq(f"SELECT COALESCE(SUM(monto),0) as t FROM ingresos WHERE {w}",one=True)
        g = dbq(f"SELECT COALESCE(SUM(monto),0) as t FROM gastos WHERE {w}",one=True)
        finanzas = {"ingresos":i["t"],"gastos":g["t"],"neto":i["t"]-g["t"]}
    return render_template("dashboard.html", conteo=conteo, alertas=alertas,
        recientes=recientes, finanzas=finanzas, fmtQ=fmtQ, hoy=hoy)

# ── Escrituras ───────────────────────────────────────────────────────
def sig_numero():
    row = dbq("SELECT numero FROM escrituras ORDER BY id DESC LIMIT 1",one=True)
    if row:
        try: return str(int(row["numero"])+1).zfill(5)
        except: pass
    nums = []
    for r in dbq("SELECT numero FROM escrituras"):
        try: nums.append(int(r["numero"]))
        except: pass
    return str((max(nums)+1) if nums else 1).zfill(5)

@app.route("/escrituras")
@login_required
def escrituras():
    estado = request.args.get("estado","Todos")
    busca  = request.args.get("busca","").strip()
    where,params = [],[]
    if estado and estado!="Todos": where.append("estado=?"); params.append(estado)
    if busca:
        where.append("(cliente LIKE ? OR numero LIKE ? OR tipo LIKE ?)")
        params += [f"%{busca}%"]*3
    sql = "SELECT * FROM escrituras"
    if where: sql += " WHERE "+" AND ".join(where)
    sql += " ORDER BY fecha DESC, id DESC"
    rows = dbq(sql,params)
    conteo = {}
    for e in ESTADOS_E:
        r = dbq("SELECT COUNT(*) as n FROM escrituras WHERE estado=?",(e,),one=True)
        conteo[e] = r["n"] if r else 0
    return render_template("escrituras.html", rows=rows, conteo=conteo,
        estados=ESTADOS_E, estado_sel=estado, busca=busca, fmtQ=fmtQ)

@app.route("/escrituras/nueva", methods=["GET","POST"])
@login_required
def escritura_nueva():
    if request.method=="POST":
        d = request.form
        try:
            ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            dbx("""INSERT INTO escrituras(numero,tipo,cliente,descripcion,fecha,estado,honorarios,
                notas,fecha_reg_mercantil,hora_reg_mercantil,avance_reg,porcentaje_avance,num_documento,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (d["numero"],d["tipo"],d["cliente"],d.get("descripcion",""),d["fecha"],d["estado"],
                 float(d.get("honorarios",0) or 0),d.get("notas",""),
                 d.get("fecha_reg_mercantil",""),d.get("hora_reg_mercantil",""),
                 d.get("avance_reg",""),int(d.get("porcentaje_avance",0) or 0),d.get("num_documento",""),ts,ts))
            flash("Escritura guardada.","ok")
            return redirect(url_for("escrituras"))
        except Exception as ex: flash(f"Error: {ex}","error")
    clientes = dbq("SELECT nombre FROM clientes WHERE activo=1 ORDER BY nombre")
    return render_template("escritura_form.html", e=None, siguiente=sig_numero(),
        estados=ESTADOS_E, tipos=TIPOS_E, clientes=clientes, hoy=datetime.date.today().isoformat())

@app.route("/escrituras/<int:eid>/editar", methods=["GET","POST"])
@login_required
def escritura_editar(eid):
    e = dbq("SELECT * FROM escrituras WHERE id=?",(eid,),one=True)
    if not e: return redirect(url_for("escrituras"))
    if request.method=="POST":
        d = request.form
        try:
            ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            dbx("""UPDATE escrituras SET numero=?,tipo=?,cliente=?,descripcion=?,fecha=?,estado=?,
                honorarios=?,notas=?,fecha_reg_mercantil=?,hora_reg_mercantil=?,avance_reg=?,
                porcentaje_avance=?,num_documento=?,updated_at=? WHERE id=?""",
                (d["numero"],d["tipo"],d["cliente"],d.get("descripcion",""),d["fecha"],d["estado"],
                 float(d.get("honorarios",0) or 0),d.get("notas",""),
                 d.get("fecha_reg_mercantil",""),d.get("hora_reg_mercantil",""),
                 d.get("avance_reg",""),int(d.get("porcentaje_avance",0) or 0),d.get("num_documento",""),ts,eid))
            flash("Escritura actualizada.","ok"); return redirect(url_for("escrituras"))
        except Exception as ex: flash(f"Error: {ex}","error")
    clientes = dbq("SELECT nombre FROM clientes WHERE activo=1 ORDER BY nombre")
    return render_template("escritura_form.html", e=e, siguiente=None,
        estados=ESTADOS_E, tipos=TIPOS_E, clientes=clientes, hoy=datetime.date.today().isoformat())

@app.route("/escrituras/<int:eid>/estado", methods=["POST"])
@login_required
def escritura_estado(eid):
    estado = request.form.get("estado")
    if estado in ESTADOS_E:
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dbx("UPDATE escrituras SET estado=?,updated_at=? WHERE id=?",(estado,ts,eid))
    return redirect(request.referrer or url_for("escrituras"))

@app.route("/escrituras/<int:eid>/eliminar", methods=["POST"])
@login_required
def escritura_eliminar(eid):
    dbx("DELETE FROM escrituras WHERE id=?",(eid,))
    flash("Escritura eliminada.","ok"); return redirect(url_for("escrituras"))

# ── Clientes ─────────────────────────────────────────────────────────
@app.route("/clientes")
@login_required
def clientes():
    busca = request.args.get("busca","").strip()
    sa = request.args.get("activos","1")=="1"
    where,params = [],[]
    if sa: where.append("activo=1")
    if busca:
        where.append("(nombre LIKE ? OR telefono LIKE ? OR whatsapp LIKE ? OR dpi LIKE ?)")
        params += [f"%{busca}%"]*4
    sql = "SELECT * FROM clientes"
    if where: sql += " WHERE "+" AND ".join(where)
    sql += " ORDER BY nombre ASC"
    return render_template("clientes.html", rows=dbq(sql,params), busca=busca, solo_activos=sa)

@app.route("/clientes/nuevo", methods=["GET","POST"])
@login_required
def cliente_nuevo():
    if request.method=="POST":
        d = request.form
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dbx("INSERT INTO clientes(nombre,telefono,whatsapp,email,dpi,direccion,ocupacion,notas,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (d["nombre"],d.get("telefono",""),d.get("whatsapp",""),d.get("email",""),
             d.get("dpi",""),d.get("direccion",""),d.get("ocupacion",""),d.get("notas",""),ts,ts))
        flash("Cliente guardado.","ok"); return redirect(url_for("clientes"))
    return render_template("cliente_form.html", c=None, ocupaciones=OCUP, historial=[])

@app.route("/clientes/<int:cid>/editar", methods=["GET","POST"])
@login_required
def cliente_editar(cid):
    c = dbq("SELECT * FROM clientes WHERE id=?",(cid,),one=True)
    if not c: return redirect(url_for("clientes"))
    if request.method=="POST":
        d = request.form
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dbx("""UPDATE clientes SET nombre=?,telefono=?,whatsapp=?,email=?,dpi=?,
            direccion=?,ocupacion=?,notas=?,updated_at=? WHERE id=?""",
            (d["nombre"],d.get("telefono",""),d.get("whatsapp",""),d.get("email",""),
             d.get("dpi",""),d.get("direccion",""),d.get("ocupacion",""),d.get("notas",""),ts,cid))
        flash("Cliente actualizado.","ok"); return redirect(url_for("clientes"))
    historial = dbq("SELECT * FROM escrituras WHERE cliente=? ORDER BY fecha DESC",(c["nombre"],))
    return render_template("cliente_form.html", c=c, ocupaciones=OCUP, historial=historial, fmtQ=fmtQ)

@app.route("/clientes/<int:cid>/eliminar", methods=["POST"])
@login_required
def cliente_eliminar(cid):
    dbx("UPDATE clientes SET activo=0 WHERE id=?",(cid,))
    flash("Cliente desactivado.","ok"); return redirect(url_for("clientes"))

@app.route("/api/clientes")
@login_required
def api_clientes():
    q = request.args.get("q","")
    rows = dbq("SELECT nombre FROM clientes WHERE activo=1 AND nombre LIKE ? ORDER BY nombre LIMIT 20",(f"%{q}%",))
    return jsonify([r["nombre"] for r in rows])

# ── Ingresos ─────────────────────────────────────────────────────────
@app.route("/ingresos")
@admin_required
def ingresos():
    rango = request.args.get("rango","Este mes")
    desde = request.args.get("desde",""); hasta = request.args.get("hasta","")
    w = rango_sql(rango,desde,hasta)
    rows  = dbq(f"SELECT * FROM ingresos WHERE {w} ORDER BY fecha DESC, id DESC")
    total = dbq(f"SELECT COALESCE(SUM(monto),0) as t FROM ingresos WHERE {w}",one=True)["t"]
    return render_template("ingresos.html", rows=rows, total=total, rango=rango,
        desde=desde, hasta=hasta, fmtQ=fmtQ, conceptos=CONC_ING,
        hoy=datetime.date.today().isoformat(), rangos=RANGOS)

@app.route("/ingresos/nuevo", methods=["POST"])
@admin_required
def ingreso_nuevo():
    d = request.form
    try:
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dbx("INSERT INTO ingresos(fecha,concepto,cliente,monto,nota,created_at) VALUES(?,?,?,?,?,?)",
            (d["fecha"],d["concepto"],d.get("cliente",""),float(d["monto"]),d.get("nota",""),ts))
        flash("Ingreso registrado.","ok")
    except Exception as ex: flash(f"Error: {ex}","error")
    return redirect(url_for("ingresos"))

@app.route("/ingresos/<int:iid>/eliminar", methods=["POST"])
@admin_required
def ingreso_eliminar(iid):
    dbx("DELETE FROM ingresos WHERE id=?",(iid,))
    flash("Ingreso eliminado.","ok"); return redirect(url_for("ingresos"))

# ── Gastos ───────────────────────────────────────────────────────────
@app.route("/gastos")
@admin_required
def gastos():
    rango = request.args.get("rango","Este mes")
    desde = request.args.get("desde",""); hasta = request.args.get("hasta","")
    w = rango_sql(rango,desde,hasta)
    rows  = dbq(f"SELECT * FROM gastos WHERE {w} ORDER BY fecha DESC, id DESC")
    total = dbq(f"SELECT COALESCE(SUM(monto),0) as t FROM gastos WHERE {w}",one=True)["t"]
    return render_template("gastos.html", rows=rows, total=total, rango=rango,
        desde=desde, hasta=hasta, fmtQ=fmtQ, conceptos=CONC_GAS,
        hoy=datetime.date.today().isoformat(), rangos=RANGOS)

@app.route("/gastos/nuevo", methods=["POST"])
@admin_required
def gasto_nuevo():
    d = request.form
    try:
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dbx("INSERT INTO gastos(fecha,concepto,descripcion,monto,nota,created_at) VALUES(?,?,?,?,?,?)",
            (d["fecha"],d["concepto"],d.get("descripcion",""),float(d["monto"]),d.get("nota",""),ts))
        flash("Gasto registrado.","ok")
    except Exception as ex: flash(f"Error: {ex}","error")
    return redirect(url_for("gastos"))

@app.route("/gastos/<int:gid>/eliminar", methods=["POST"])
@admin_required
def gasto_eliminar(gid):
    dbx("DELETE FROM gastos WHERE id=?",(gid,))
    flash("Gasto eliminado.","ok"); return redirect(url_for("gastos"))

# ── Reportes ─────────────────────────────────────────────────────────
@app.route("/reportes")
@admin_required
def reportes():
    rango = request.args.get("rango","Este mes")
    desde = request.args.get("desde",""); hasta = request.args.get("hasta","")
    w = rango_sql(rango,desde,hasta)
    it = dbq(f"SELECT COALESCE(SUM(monto),0) as t FROM ingresos WHERE {w}",one=True)["t"]
    gt = dbq(f"SELECT COALESCE(SUM(monto),0) as t FROM gastos WHERE {w}",one=True)["t"]
    ic = dbq(f"SELECT concepto,SUM(monto) as total FROM ingresos WHERE {w} GROUP BY concepto ORDER BY total DESC")
    gc = dbq(f"SELECT concepto,SUM(monto) as total FROM gastos WHERE {w} GROUP BY concepto ORDER BY total DESC")
    ee = dbq("SELECT estado,COUNT(*) as n FROM escrituras GROUP BY estado")
    return render_template("reportes.html", ing_total=it, gas_total=gt, neto=it-gt,
        ing_concepto=ic, gas_concepto=gc, escrituras_estado=ee,
        rango=rango, desde=desde, hasta=hasta, fmtQ=fmtQ, rangos=RANGOS)

# ── Usuarios ─────────────────────────────────────────────────────────
@app.route("/usuarios")
@admin_required
def usuarios():
    return render_template("usuarios.html", rows=dbq("SELECT id,username,rol,activo FROM usuarios ORDER BY id"))

@app.route("/usuarios/nuevo", methods=["POST"])
@admin_required
def usuario_nuevo():
    d = request.form
    try:
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dbx("INSERT INTO usuarios(username,password,rol,created_at) VALUES(?,?,?,?)",
            (d["username"],hashlib.sha256(d["password"].encode()).hexdigest(),d["rol"],ts))
        flash("Usuario creado.","ok")
    except Exception as ex: flash(f"Error: {ex}","error")
    return redirect(url_for("usuarios"))

@app.route("/usuarios/<int:uid>/toggle", methods=["POST"])
@admin_required
def usuario_toggle(uid):
    u = dbq("SELECT activo FROM usuarios WHERE id=?",(uid,),one=True)
    if u: dbx("UPDATE usuarios SET activo=? WHERE id=?",(0 if u["activo"] else 1,uid))
    return redirect(url_for("usuarios"))

@app.route("/usuarios/<int:uid>/pass", methods=["POST"])
@admin_required
def usuario_pass(uid):
    p = request.form.get("password","")
    if p:
        dbx("UPDATE usuarios SET password=? WHERE id=?",(hashlib.sha256(p.encode()).hexdigest(),uid))
        flash("Contraseña actualizada.","ok")
    return redirect(url_for("usuarios"))

# ── Respaldos ─────────────────────────────────────────────────────────
@app.route("/respaldos")
@login_required
def respaldos():
    return render_template("respaldos.html", rows=dbq("SELECT * FROM respaldos ORDER BY id DESC"))

@app.route("/respaldos/crear", methods=["POST"])
@login_required
def respaldo_crear():
    if USE_POSTGRES:
        flash("Los respaldos automáticos se gestionan desde el panel de Railway → PostgreSQL → Backups.","ok")
        return redirect(url_for("respaldos"))
    import sqlite3 as _sq
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    n  = (dbq("SELECT MAX(id) as m FROM respaldos",one=True)["m"] or 0) + 1
    nombre = f"respaldo{n}_{ts}.db"
    dest = BACKUP_DIR / nombre
    src = get_db(); dst = _sq.connect(str(dest))
    src.backup(dst); dst.close(); src.close()
    tam = round(dest.stat().st_size/1024,2)
    dbx("INSERT INTO respaldos(nombre,ruta,tamanio_kb) VALUES(?,?,?)",(nombre,str(dest),tam))
    flash(f"Respaldo creado: {nombre}","ok"); return redirect(url_for("respaldos"))

# ── Clave del programador para descargar respaldos ──────────────────
CLAVE_PROGRAMADOR = hashlib.sha256("123456789abcdefghi####".encode()).hexdigest()

@app.route("/respaldos/<int:rid>/descargar", methods=["GET","POST"])
@login_required
def respaldo_descargar(rid):
    if request.method == "POST":
        clave = request.form.get("clave_prog","")
        if hashlib.sha256(clave.encode()).hexdigest() != CLAVE_PROGRAMADOR:
            flash("Clave incorrecta. No tiene autorización para descargar respaldos.","error")
            return redirect(url_for("respaldos"))
        row = dbq("SELECT * FROM respaldos WHERE id=?",(rid,),one=True)
        if row and Path(row["ruta"]).exists():
            return send_file(row["ruta"], as_attachment=True, download_name=row["nombre"])
        flash("Archivo no encontrado.","error")
        return redirect(url_for("respaldos"))
    # GET: mostrar formulario de clave
    row = dbq("SELECT * FROM respaldos WHERE id=?",(rid,),one=True)
    if not row:
        flash("Respaldo no encontrado.","error")
        return redirect(url_for("respaldos"))
    return render_template("respaldo_clave.html", rid=rid, nombre=row["nombre"])

@app.route("/respaldos/<int:rid>/eliminar", methods=["POST"])
@login_required
def respaldo_eliminar(rid):
    row = dbq("SELECT ruta FROM respaldos WHERE id=?",(rid,),one=True)
    if row:
        p = Path(row["ruta"])
        if p.exists(): p.unlink()
    dbx("DELETE FROM respaldos WHERE id=?",(rid,))
    flash("Respaldo eliminado.","ok"); return redirect(url_for("respaldos"))

# ── Main ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    app.run(debug=True, host="0.0.0.0", port=5000)

# ═══════════════════════════════════════════════════════════════════════
# NUEVAS FUNCIONES — v3.0
# ═══════════════════════════════════════════════════════════════════════

# ── 1. BÚSQUEDA GLOBAL ──────────────────────────────────────────────
@app.route("/buscar")
@login_required
def buscar_global():
    q = request.args.get("q", "").strip()
    resultados = {"escrituras": [], "clientes": [], "total": 0}
    if q and len(q) >= 2:
        pat = f"%{q}%"
        resultados["escrituras"] = dbq(
            "SELECT id,numero,tipo,cliente,estado,fecha,honorarios FROM escrituras "
            "WHERE numero LIKE ? OR cliente LIKE ? OR tipo LIKE ? OR descripcion LIKE ? "
            "ORDER BY fecha DESC LIMIT 15", (pat,pat,pat,pat))
        resultados["clientes"] = dbq(
            "SELECT id,nombre,telefono,whatsapp,dpi FROM clientes "
            "WHERE activo=1 AND (nombre LIKE ? OR telefono LIKE ? OR whatsapp LIKE ? OR dpi LIKE ?) "
            "ORDER BY nombre LIMIT 10", (pat,pat,pat,pat))
        resultados["total"] = len(resultados["escrituras"]) + len(resultados["clientes"])
    return render_template("buscar.html", q=q, res=resultados, fmtQ=fmtQ)

@app.route("/api/buscar")
@login_required
def api_buscar():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])
    pat = f"%{q}%"
    e = dbq("SELECT numero,cliente,tipo,estado FROM escrituras WHERE numero LIKE ? OR cliente LIKE ? LIMIT 5", (pat,pat))
    c = dbq("SELECT nombre,telefono FROM clientes WHERE activo=1 AND nombre LIKE ? LIMIT 5", (pat,))
    out = []
    for r in e:
        out.append({"tipo":"escritura","titulo":f"Escritura #{r['numero']}","sub":f"{r['cliente']} · {r['estado']}","url":f"/escrituras"})
    for r in c:
        out.append({"tipo":"cliente","titulo":r["nombre"],"sub":r["telefono"] or "Sin teléfono","url":f"/clientes"})
    return jsonify(out)

# ── 2. CALENDARIO DE VENCIMIENTOS ───────────────────────────────────
@app.route("/calendario")
@login_required
def calendario():
    hoy = datetime.date.today()
    # Próximos 60 días de escrituras en proceso con fecha reg mercantil
    limite_ini = hoy.isoformat()
    limite_fin = (hoy + datetime.timedelta(days=60)).isoformat()
    proximas = dbq(
        "SELECT id,numero,cliente,tipo,estado,fecha_reg_mercantil,honorarios FROM escrituras "
        "WHERE fecha_reg_mercantil!='' AND fecha_reg_mercantil>=? "
        "AND estado NOT IN ('Entregado al Cliente') "
        "ORDER BY fecha_reg_mercantil ASC LIMIT 50", (limite_ini,))
    vencidas = dbq(
        "SELECT id,numero,cliente,tipo,estado,fecha_reg_mercantil FROM escrituras "
        "WHERE fecha_reg_mercantil!='' AND fecha_reg_mercantil<? "
        "AND estado NOT IN ('Listo para Retirar','Entregado al Cliente') "
        "ORDER BY fecha_reg_mercantil DESC LIMIT 30", (limite_ini,))
    # Agrupar por mes para el calendario visual
    meses = {}
    for e in proximas:
        try:
            d = datetime.date.fromisoformat(e["fecha_reg_mercantil"])
            clave = d.strftime("%Y-%m")
            meses.setdefault(clave, []).append(e)
        except: pass
    return render_template("calendario.html", proximas=proximas, vencidas=vencidas,
        meses=meses, hoy=hoy.isoformat(), fmtQ=fmtQ)

# ── 3. EXPORTAR EXCEL / CSV ─────────────────────────────────────────
@app.route("/exportar/escrituras")
@login_required
def exportar_escrituras():
    import csv, io
    rows = dbq("SELECT numero,tipo,cliente,fecha,estado,honorarios,fecha_reg_mercantil,avance_reg,notas FROM escrituras ORDER BY fecha DESC, id DESC")
    buf = io.StringIO()
    buf.write('\ufeff')  # BOM para Excel
    w = csv.writer(buf)
    w.writerow(["#","Tipo","Cliente","Fecha","Estado","Honorarios Q","Reg.Mercantil","Avance","Notas"])
    for r in rows:
        w.writerow([r["numero"],r["tipo"],r["cliente"],r["fecha"],r["estado"],
                    f"{r['honorarios']:.2f}",r["fecha_reg_mercantil"] or "",
                    r["avance_reg"] or "",r["notas"] or ""])
    buf.seek(0)
    return send_file(
        io.BytesIO(buf.getvalue().encode("utf-8-sig")),
        mimetype="text/csv; charset=utf-8",
        as_attachment=True,
        download_name=f"escrituras_{datetime.date.today().isoformat()}.csv")

@app.route("/exportar/clientes")
@login_required
def exportar_clientes():
    import csv, io
    rows = dbq("SELECT nombre,telefono,whatsapp,email,dpi,ocupacion,notas FROM clientes WHERE activo=1 ORDER BY nombre")
    buf = io.StringIO()
    buf.write('\ufeff')
    w = csv.writer(buf)
    w.writerow(["Nombre","Teléfono","WhatsApp","Email","DPI","Ocupación","Notas"])
    for r in rows:
        w.writerow([r["nombre"],r["telefono"] or "",r["whatsapp"] or "",
                    r["email"] or "",r["dpi"] or "",r["ocupacion"] or "",r["notas"] or ""])
    buf.seek(0)
    return send_file(
        io.BytesIO(buf.getvalue().encode("utf-8-sig")),
        mimetype="text/csv; charset=utf-8",
        as_attachment=True,
        download_name=f"clientes_{datetime.date.today().isoformat()}.csv")

@app.route("/exportar/finanzas")
@admin_required
def exportar_finanzas():
    import csv, io
    buf = io.StringIO()
    buf.write('\ufeff')
    w = csv.writer(buf)
    w.writerow(["INGRESOS"])
    w.writerow(["Fecha","Concepto","Cliente","Monto Q","Nota"])
    for r in dbq("SELECT fecha,concepto,cliente,monto,nota FROM ingresos ORDER BY fecha DESC"):
        w.writerow([r["fecha"],r["concepto"],r["cliente"] or "",f"{r['monto']:.2f}",r["nota"] or ""])
    w.writerow([])
    w.writerow(["GASTOS"])
    w.writerow(["Fecha","Concepto","Descripción","Monto Q","Nota"])
    for r in dbq("SELECT fecha,concepto,descripcion,monto,nota FROM gastos ORDER BY fecha DESC"):
        w.writerow([r["fecha"],r["concepto"],r["descripcion"] or "",f"{r['monto']:.2f}",r["nota"] or ""])
    buf.seek(0)
    return send_file(
        io.BytesIO(buf.getvalue().encode("utf-8-sig")),
        mimetype="text/csv; charset=utf-8",
        as_attachment=True,
        download_name=f"finanzas_{datetime.date.today().isoformat()}.csv")

# ── 4. WHATSAPP HELPER ──────────────────────────────────────────────
@app.route("/whatsapp/mensaje")
@login_required
def whatsapp_mensaje():
    """Genera el link de WhatsApp con el mensaje listo para enviar."""
    import urllib.parse
    tipo  = request.args.get("tipo","")
    eid   = request.args.get("eid","")
    cid   = request.args.get("cid","")
    nombre = request.args.get("nombre","")
    numero = request.args.get("numero","")
    estado = request.args.get("estado","")
    tel    = request.args.get("tel","").strip().replace(" ","").replace("-","")

    plantillas = {
        "listo": f"Estimado/a {nombre}, le informamos que su escritura #{numero} ya está lista para retirar en nuestra oficina. Por favor, preséntese en horario de atención. Gracias. — Enriquez Flores & Asociados",
        "seguimiento": f"Estimado/a {nombre}, le informamos sobre el estado de su escritura #{numero}: *{estado}*. Si tiene alguna consulta no dude en contactarnos. — Enriquez Flores & Asociados",
        "recordatorio": f"Estimado/a {nombre}, le recordamos que tiene trámites pendientes en nuestra oficina. Por favor comuníquese para coordinar. — Enriquez Flores & Asociados",
        "bienvenida": f"Bienvenido/a {nombre} a Enriquez Flores & Asociados. Quedamos a sus órdenes para cualquier consulta jurídica. — Oficina Jurídica Guatemala",
    }
    tipo_msg = request.args.get("plantilla","seguimiento")
    mensaje = plantillas.get(tipo_msg, plantillas["seguimiento"])
    # Limpiar número (agregar 502 si es Guatemala)
    if tel and not tel.startswith("+") and not tel.startswith("502"):
        tel = "502" + tel
    tel = tel.replace("+","")
    link = f"https://wa.me/{tel}?text={urllib.parse.quote(mensaje)}"
    return redirect(link)

@app.route("/api/whatsapp_link")
@login_required
def api_whatsapp_link():
    import urllib.parse
    tel      = request.args.get("tel","").strip().replace(" ","").replace("-","")
    mensaje  = request.args.get("msg","Hola, contacto desde Oficina Jurídica Enriquez Flores.")
    if tel and not tel.startswith("502") and len(tel) == 8:
        tel = "502" + tel
    link = f"https://wa.me/{tel}?text={urllib.parse.quote(mensaje)}"
    return jsonify({"link": link})
