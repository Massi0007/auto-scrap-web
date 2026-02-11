import os
import math
import re
from datetime import datetime
from flask import Flask, render_template, request, session, redirect, url_for, g
import psycopg2
from psycopg2 import pool, extras

app = Flask(__name__)

# --- CONFIGURATION SÉCURISÉE ---
app.secret_key = os.environ.get("SECRET_KEY", "dev_secret_key_change_in_prod")
SUPABASE_DSN = os.environ.get("DATABASE_URL", "postgresql://postgres.kbmersivclctgwenawbl:PWDDB112358%40%40@aws-1-eu-central-1.pooler.supabase.com:6543/postgres?tcp_user_timeout=10000&keepalives=1&keepalives_idle=30")

# --- DATABASE POOL ---
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, dsn=SUPABASE_DSN)
    print("✅ Pool de connexion Supabase initialisé.")
except Exception as e:
    print(f"❌ Erreur critique connexion Supabase : {e}")
    exit()

def get_db():
    if 'db' not in g:
        try:
            conn = db_pool.getconn()
            with conn.cursor() as cur: cur.execute('SELECT 1')
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            try: db_pool.putconn(conn, close=True)
            except: pass
            conn = db_pool.getconn()
        g.db = conn
        g.db.cursor_factory = extras.RealDictCursor
    return g.db

@app.teardown_appcontext
def close_db(e):
    db = g.pop('db', None)
    if db is not None: db_pool.putconn(db)

# --- FILTRES JINJA ---
@app.template_filter('format_number')
def format_number_filter(value):
    if value is None or value == "": return "N/A"
    try: return "{:,}".format(int(float(value))).replace(",", " ")
    except: return value

@app.template_filter('format_percent')
def format_percent_filter(value):
    if value is None: return "0%"
    try: return "{:.1f}%".format(float(value))
    except: return "0%"

@app.template_filter('format_datetime')
def format_datetime_filter(value, fmt='%d/%m à %H:%M'):
    if not value: return ""
    try:
        if isinstance(value, str):
            clean_date = value.split('.')[0].replace('Z', '').replace('+00', '')
            if 'T' in clean_date: dt = datetime.fromisoformat(clean_date)
            else: dt = datetime.strptime(clean_date, '%Y-%m-%d %H:%M:%S')
            return dt.strftime(fmt)
        return value.strftime(fmt)
    except: return str(value)

# --- HELPER URL LEBONCOIN ---
@app.template_global()
def build_leboncoin_url(a):
    base_url = "https://www.leboncoin.fr/recherche?category=2"
    params = []
    try:
        data = dict(a)
        marque = data.get('marque_annonce', '')
        if marque: params.append(f"u_car_brand={marque.upper()}")
        modele = data.get('modele_annonce', '')
        if marque and modele:
            modele_fmt = modele.replace(' ', '_').capitalize()
            params.append(f"u_car_model={marque.upper()}_{modele_fmt}")
        elif modele: params.append(f"keywords={modele}")
        
        annee = data.get('annee')
        if annee and str(annee).isdigit() and int(annee) > 1900:
            y = int(annee)
            params.append(f"regdate={y-1}-{y+1}")
        
        km = data.get('kilometrage')
        if km: 
            try: val_km = int(float(km)); params.append(f"mileage={max(0, val_km-10000)}-{val_km+10000}")
            except: pass
            
        energie = str(data.get('energie', '')).lower()
        if 'essence' in energie: params.append("fuel=1")
        elif 'diesel' in energie: params.append("fuel=2")
        elif 'électrique' in energie: params.append("fuel=4")
        elif 'hybride' in energie: params.append("fuel=6")

        boite = str(data.get('boite_de_vitesse', '')).lower()
        if 'manuelle' in boite: params.append("gearbox=1")
        elif 'auto' in boite: params.append("gearbox=2")
    except: pass
    return base_url + "&" + "&".join(params) if params else base_url

# --- ROUTES ---

@app.route('/ventes')
def ventes():
    """Page catalogue des ventes avec statistiques groupées"""
    conn = get_db()
    cursor = conn.cursor()
    
    # Agrégation des stats par vente + Récupération d'une image représentative
    query = """
        SELECT 
            vente_id, 
            nom_vente, 
            date_debut, 
            ville, 
            organisateur,
            COUNT(*) as nb_lots,
            MAX(image_principale) as image_vente, -- On prend une image au hasard pour illustrer la vente
            SUM(CASE WHEN marge_estimee_min > 0 THEN marge_estimee_min ELSE 0 END) as potentiel_marge_total,
            AVG(CASE WHEN prix_max_frais_inclus > 0 THEN (marge_estimee_min / prix_max_frais_inclus) * 100 ELSE 0 END) as roi_moyen
        FROM vue_details_ventes_avenir
        GROUP BY vente_id, nom_vente, date_debut, ville, organisateur
        ORDER BY date_debut ASC
    """
    try:
        cursor.execute(query)
        ventes_list = cursor.fetchall()
    except Exception as e:
        print(f"Erreur SQL Ventes: {e}")
        conn.rollback()
        ventes_list = []
        
    return render_template('ventes.html', ventes=ventes_list)


@app.route('/', methods=['GET', 'POST'])
def index():
    conn = get_db()
    cursor = conn.cursor()
    
    # 1. Filtres Session
    if request.method == 'POST':
        filters_data = request.form.to_dict(flat=False)
        clean_filters = {k: [x for x in v if x.strip()] for k, v in filters_data.items() if any(x.strip() for x in v)}
        session['filters'] = clean_filters
        return redirect(url_for('index', vente_id=request.args.get('vente_id')))

    filters = session.get('filters', {})
    
    # 2. Paramètre GET spécifique pour filtrer par Vente (depuis la page /ventes)
    get_vente_id = request.args.get('vente_id')
    
    page = request.args.get('page', 1, type=int)
    per_page = 40

    # Variables Filtres
    f_keyword = filters.get('keyword', [''])[0]
    f_sort = filters.get('sort', ['roi_desc'])[0]
    f_marques = filters.get('marque', [])
    f_maisons = filters.get('maison_vente', [])
    f_energies = filters.get('energie', [])
    f_boites = filters.get('boite', [])
    
    f_annee_min = filters.get('annee_min', [''])[0]
    f_annee_max = filters.get('annee_max', [''])[0]
    f_km_max = filters.get('km_max', [''])[0]
    f_prix_max = filters.get('prix_max', [''])[0]
    f_marge_min = filters.get('marge_min', [''])[0]
    f_date_min = filters.get('date_min', [''])[0]
    f_date_max = filters.get('date_max', [''])[0]

    # Helpers: Modifié pour filtrer par vente si nécessaire
    def safe_fetch_list(field, v_id=None):
        try:
            query_str = f"SELECT DISTINCT {field} FROM vue_details_ventes_avenir WHERE {field} IS NOT NULL"
            query_params = []
            
            if v_id:
                query_str += " AND vente_id = %s"
                query_params.append(v_id)
                
            query_str += " ORDER BY 1"
            
            cursor.execute(query_str, query_params)
            return [r[field] for r in cursor.fetchall()]
        except: return []

    marques = safe_fetch_list('marque_annonce', get_vente_id)
    maisons = safe_fetch_list('organisateur', get_vente_id)
    energies = safe_fetch_list('energie', get_vente_id)
    boites = safe_fetch_list('boite_de_vitesse', get_vente_id)
    
    ranges = {'annee_min': 2000, 'annee_max': datetime.now().year + 1, 'km_max': 250000, 'prix_max': 60000}

    # Query Builder
    base_sql = """
        SELECT *, 
        annonce_id AS id,
        marque_annonce || ' ' || modele_annonce AS titre_annonce,
        organisateur AS maison_vente,
        date_debut AS date_vente_ts,
        marge_estimee_min AS marge,
        image_principale AS lien_image_vehicule
        FROM vue_details_ventes_avenir
        WHERE 1=1
    """
    params = []
    
    # -- NOUVEAU : Filtre par Vente ID --
    if get_vente_id:
        base_sql += " AND vente_id = %s"
        params.append(get_vente_id)

    # Filtres standards
    if f_keyword:
        kw = f"%{f_keyword}%"
        base_sql += " AND (marque_annonce ILIKE %s OR modele_annonce ILIKE %s OR description ILIKE %s)"
        params.extend([kw, kw, kw])

    if f_marques:
        base_sql += f" AND marque_annonce IN ({','.join(['%s']*len(f_marques))})"; params.extend(f_marques)
    if f_maisons:
        base_sql += f" AND organisateur IN ({','.join(['%s']*len(f_maisons))})"; params.extend(f_maisons)
    if f_energies:
        base_sql += f" AND energie IN ({','.join(['%s']*len(f_energies))})"; params.extend(f_energies)

    if f_boites:
        has_none, clean_boites = 'None' in f_boites, [b for b in f_boites if b != 'None']
        c = []
        if clean_boites: c.append(f"boite_de_vitesse IN ({','.join(['%s']*len(clean_boites))})"); params.extend(clean_boites)
        if has_none: c.append("(boite_de_vitesse IS NULL OR boite_de_vitesse = '')")
        if c: base_sql += " AND (" + " OR ".join(c) + ")"

    # Filtres Numériques (Cast Numeric explicite pour la sécurité)
    def add_num(col, val, op="<="):
        if val and str(val).isdigit(): return f" AND CAST({col} AS NUMERIC) {op} %s", int(val)
        return "", None

    for col, val, op in [("annee", f_annee_min, ">="), ("annee", f_annee_max, "<="), ("kilometrage", f_km_max, "<="), ("prix_max_frais_inclus", f_prix_max, "<=")]:
        s, p = add_num(col, val, op)
        if s: base_sql += s; params.append(p)
    
    if f_marge_min:
        try: base_sql += " AND CAST(marge_estimee_min AS NUMERIC) >= %s"; params.append(float(f_marge_min))
        except: pass
        
    if f_date_min: base_sql += " AND date_debut >= %s"; params.append(f_date_min)
    if f_date_max: base_sql += " AND date_debut <= %s"; params.append(f_date_max)

    # Count
    try:
        cursor.execute(f"SELECT COUNT(*) as count FROM ({base_sql}) as subquery", params)
        total_annonces = cursor.fetchone()['count']
    except: 
        conn.rollback(); total_annonces = 0

    # Tri & Pagination
    sort_map = {
        'date_vente_asc': 'ORDER BY date_debut ASC',
        'marge_desc': 'ORDER BY CAST(marge_estimee_min AS NUMERIC) DESC NULLS LAST',
        'prix_asc': 'ORDER BY CAST(prix_max_frais_inclus AS NUMERIC) ASC',
        'roi_desc': 'ORDER BY (CAST(marge_estimee_min AS NUMERIC) / NULLIF(CAST(prix_max_frais_inclus AS NUMERIC), 0)) DESC NULLS LAST' 
    }
    full_query = f"{base_sql} {sort_map.get(f_sort, sort_map['roi_desc'])} LIMIT %s OFFSET %s"
    
    try:
        cursor.execute(full_query, params + [per_page, (page - 1) * per_page])
        annonces = cursor.fetchall()
    except Exception as e:
        print(f"Erreur Fetch: {e}")
        conn.rollback()
        annonces = []

    return render_template('index.html', 
        annonces=annonces, marques=marques, maisons=maisons, energies=energies, boites=boites, ranges=ranges,
        current_page=page, total_pages=math.ceil(total_annonces/per_page) if total_annonces else 1, total_annonces=total_annonces,
        f_keyword=f_keyword, f_sort=f_sort, f_marques=f_marques, f_maisons=f_maisons, f_energies=f_energies, f_boites=f_boites,
        f_annee_min=f_annee_min, f_annee_max=f_annee_max, f_km_max=f_km_max, f_prix_max=f_prix_max, f_marge_min=f_marge_min,
        f_date_min=f_date_min, f_date_max=f_date_max,
        current_vente_id=get_vente_id
    )

@app.route('/reset_filters')
def reset_filters():
    session.pop('filters', None)
    return redirect(url_for('index'))

@app.route('/annonce/<int:annonce_id>')
def detail(annonce_id):
    conn = get_db(); cursor = conn.cursor()
    cursor.execute("SELECT *, image_principale AS lien_image_vehicule, organisateur AS maison_vente FROM vue_details_ventes_avenir WHERE annonce_id = %s", (annonce_id,))
    annonce = cursor.fetchone()
    if not annonce: return "Introuvable", 404
    
    # --- MISE À JOUR : Gestion du prix marché (Prix unique OU fourchette legacy) ---
    try: 
        fp = annonce.get('fourchette_prix_marche', '')
        # Nettoyage basique (enlève €, espaces)
        cleaned_fp = str(fp).replace('€', '').replace(' ', '').strip()
        
        if '-' in cleaned_fp:
            # Cas "Legacy" : Fourchette (ex: "4000-5000")
            parts = cleaned_fp.split('-')
            annonce['market_avg'] = (float(parts[0]) + float(parts[1])) / 2
        elif cleaned_fp:
            # Cas "Nouveau" : Prix unique (ex: "4500") ou chiffre pur
            # On vérifie si c'est un nombre valide
            if re.match(r'^\d+(\.\d+)?$', cleaned_fp):
                annonce['market_avg'] = float(cleaned_fp)
    except Exception as e: 
        # En cas d'erreur de conversion, on ignore silencieusement
        print(f"Erreur calcul market_avg: {e}")
        pass
    
    return render_template('detail.html', annonce=annonce)

if __name__ == '__main__':
    app.run(debug=True, port=5001)