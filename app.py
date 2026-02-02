import os
import math
from datetime import datetime
from flask import Flask, render_template, request, session, redirect, url_for, g
import psycopg2
from psycopg2 import pool, extras

app = Flask(__name__)
app.secret_key = "c762c135e2beb6ec46ccdbc20a563298afb19cc0bd9b7d25"
app.jinja_env.add_extension('jinja2.ext.do')

# --- CONFIGURATION SUPABASE ---
# ⚠️ Ta chaîne de connexion Pooler (IPv4)
# Remplace PWDDB112358%40%40 par ton mot de passe si ce n'est pas celui-ci
SUPABASE_DSN = "postgresql://postgres.kbmersivclctgwenawbl:PWDDB112358%40%40@aws-1-eu-central-1.pooler.supabase.com:6543/postgres?tcp_user_timeout=10000&keepalives=1&keepalives_idle=30&keepalives_interval=10&keepalives_count=5"

# Initialisation du Pool de Connexion (Global)
# minconn=1, maxconn=20
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, dsn=SUPABASE_DSN)
    print("✅ Pool de connexion Supabase initialisé.")
except Exception as e:
    print(f"❌ Erreur critique connexion Supabase : {e}")
    exit()

def get_db():
    """Récupère une connexion valide depuis le pool."""
    if 'db' not in g:
        try:
            conn = db_pool.getconn()
            # Test de la connexion : si elle est morte, on l'enlève et on en prend une nouvelle
            with conn.cursor() as cur:
                cur.execute('SELECT 1')
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            # Si la connexion est expirée, on la ferme proprement et on en crée une nouvelle
            db_pool.putconn(conn, close=True)
            conn = db_pool.getconn()
        
        g.db = conn
        g.db.cursor_factory = extras.RealDictCursor
    return g.db

@app.teardown_appcontext
def close_db(e):
    """Rend la connexion au pool à la fin de la requête."""
    db = g.pop('db', None)
    if db is not None:
        db_pool.putconn(db)

# --- HELPER URL LEBONCOIN ---
@app.template_global()
def build_leboncoin_url(a):
    base_url = "https://www.leboncoin.fr/recherche?category=2"
    params = []
    try:
        # Conversion explicite en dict car RealDictRow se comporte un peu différemment
        data = dict(a)
        marque = data.get('marque_annonce', '')
        if marque: params.append(f"u_car_brand={marque.upper()}")
        
        modele = data.get('modele_annonce', '')
        if marque and modele:
            modele_fmt = modele.replace(' ', '_').capitalize()
            params.append(f"u_car_model={marque.upper()}_{modele_fmt}")
        elif modele:
            params.append(f"keywords={modele}")

        annee = data.get('annee')
        if annee and str(annee).isdigit() and int(annee) > 1900:
            y = int(annee)
            params.append(f"regdate={y-1}-{y+1}")

        km = data.get('kilometrage')
        if km is not None:
            try:
                val_km = int(km)
                if val_km > 0: params.append(f"mileage={max(0, val_km-10000)}-{val_km+10000}")
            except: pass

        energie = str(data.get('energie', '')).lower()
        if 'essence' in energie: params.append("fuel=1")
        elif 'diesel' in energie: params.append("fuel=2")
        elif 'électrique' in energie: params.append("fuel=4")
        elif 'hybride' in energie: params.append("fuel=6")

        boite = str(data.get('boite_de_vitesse', '')).lower()
        if 'manuelle' in boite: params.append("gearbox=1")
        elif 'automatique' in boite or 'auto' in boite: params.append("gearbox=2")

    except Exception: pass
    return base_url + "&" + "&".join(params) if params else base_url

# --- FILTRES JINJA ---
@app.template_filter('format_number')
def format_number_filter(value):
    if value is None: return "N/A"
    try: return "{:,}".format(int(value)).replace(",", " ")
    except: return value

@app.template_filter('format_datetime')
def format_datetime_filter(value, fmt='%d/%m à %H:%M'):
    if not value: return ""
    # Postgres retourne des objets datetime directement, pas des strings !
    if isinstance(value, datetime):
        return value.strftime(fmt)
    try:
        dt = datetime.strptime(str(value), '%Y-%m-%d %H:%M:%S')
        return dt.strftime(fmt)
    except:
        return str(value)

# --- ROUTE PRINCIPALE ---
@app.route('/', methods=['GET', 'POST'])
def index():
    conn = get_db()
    cursor = conn.cursor()
    
    # GESTION POST
    if request.method == 'POST':
        filters_data = request.form.to_dict(flat=False)
        clean_filters = {}
        for k, v in filters_data.items():
            valid_values = [x for x in v if x.strip()]
            if valid_values:
                clean_filters[k] = valid_values
        
        session['filters'] = clean_filters
        return redirect(url_for('index'))

    # GESTION GET
    filters = session.get('filters', {})
    
    page = request.args.get('page', 1, type=int)
    per_page = 40
    
    # Récupération des filtres
    f_keyword = filters.get('keyword', [''])[0]
    f_sort = filters.get('sort', ['date_vente_asc'])[0]
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

    # Chargement listes (SQL standard compatible Postgres)
    cursor.execute("SELECT DISTINCT marque_annonce FROM vue_details_ventes_avenir WHERE marque_annonce IS NOT NULL ORDER BY 1")
    marques = [r['marque_annonce'] for r in cursor.fetchall()]

    cursor.execute("SELECT DISTINCT organisateur FROM vue_details_ventes_avenir ORDER BY 1")
    maisons = [r['organisateur'] for r in cursor.fetchall()]

    cursor.execute("SELECT DISTINCT energie FROM vue_details_ventes_avenir WHERE energie IS NOT NULL ORDER BY 1")
    energies = [r['energie'] for r in cursor.fetchall()]

    cursor.execute("SELECT DISTINCT boite_de_vitesse FROM vue_details_ventes_avenir WHERE boite_de_vitesse IS NOT NULL ORDER BY 1")
    boites = [r['boite_de_vitesse'] for r in cursor.fetchall()]
    
    cursor.execute("SELECT MIN(annee) as min_an, MAX(annee) as max_an, MAX(kilometrage) as max_km, MAX(prix_max_frais_inclus) as max_prix FROM vue_details_ventes_avenir")
    ranges = cursor.fetchone()
    r_annee_min = ranges['min_an'] if ranges['min_an'] else 1990
    r_annee_max = ranges['max_an'] if ranges['max_an'] else datetime.now().year
    r_km_max = ranges['max_km'] if ranges['max_km'] else 200000
    r_prix_max = int(ranges['max_prix']) if ranges['max_prix'] else 50000

    # Construction de la requête principale
    # Note : || est le concaténateur standard SQL (OK pour Postgres)
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

    # Adaptation SQL : ? devient %s pour psycopg2
    if f_keyword:
        kw = f"%{f_keyword}%"
        # ILIKE est spécifique à Postgres pour insensible à la casse (mieux que LIKE)
        base_sql += " AND (marque_annonce ILIKE %s OR modele_annonce ILIKE %s OR description ILIKE %s)"
        params.extend([kw, kw, kw])

    if f_marques:
        base_sql += f" AND marque_annonce IN ({','.join(['%s']*len(f_marques))})"
        params.extend(f_marques)
        
    if f_maisons:
        base_sql += f" AND organisateur IN ({','.join(['%s']*len(f_maisons))})"
        params.extend(f_maisons)

    if f_energies:
        base_sql += f" AND energie IN ({','.join(['%s']*len(f_energies))})"
        params.extend(f_energies)

    if f_boites:
        handle_none = 'None' in f_boites
        real_boites = [b for b in f_boites if b != 'None']
        conditions = []
        if real_boites:
            conditions.append(f"boite_de_vitesse IN ({','.join(['%s']*len(real_boites))})")
            params.extend(real_boites)
        if handle_none:
            conditions.append("(boite_de_vitesse IS NULL OR boite_de_vitesse = '')")
        if conditions:
            base_sql += " AND (" + " OR ".join(conditions) + ")"

    if f_annee_min and f_annee_min.isdigit():
        base_sql += " AND annee >= %s"; params.append(int(f_annee_min))
    if f_annee_max and f_annee_max.isdigit():
        base_sql += " AND annee <= %s"; params.append(int(f_annee_max))
    if f_km_max and f_km_max.isdigit():
        base_sql += " AND kilometrage <= %s"; params.append(int(f_km_max))
    if f_prix_max and f_prix_max.isdigit():
        base_sql += " AND prix_max_frais_inclus <= %s"; params.append(int(f_prix_max))
    if f_marge_min:
        try: base_sql += " AND marge_estimee_min >= %s"; params.append(float(f_marge_min))
        except ValueError: pass

    if f_date_min:
        base_sql += " AND date_debut >= %s"; params.append(f_date_min)
    if f_date_max:
        base_sql += " AND date_debut <= %s"; params.append(f_date_max)

    # Compte total
    cursor.execute(f"SELECT COUNT(*) as count FROM ({base_sql}) as subquery", params)
    total_annonces = cursor.fetchone()['count']
    
    sort_map = {
        'date_vente_asc': 'ORDER BY date_debut ASC',
        'marge_desc': 'ORDER BY marge_estimee_min DESC',
        'prix_asc': 'ORDER BY prix_max_frais_inclus ASC',
        'km_asc': 'ORDER BY kilometrage ASC'
    }
    order_clause = sort_map.get(f_sort, 'ORDER BY date_debut ASC')

    total_pages = math.ceil(total_annonces / per_page) if total_annonces > 0 else 1
    offset = (page - 1) * per_page
    
    full_query = f"{base_sql} {order_clause} LIMIT %s OFFSET %s"
    cursor.execute(full_query, params + [per_page, offset])
    annonces = cursor.fetchall()

    return render_template('index.html', 
        annonces=annonces,
        marques=marques, maisons=maisons, energies=energies, boites=boites,
        ranges={'annee_min': r_annee_min, 'annee_max': r_annee_max, 'km_max': r_km_max, 'prix_max': r_prix_max},
        current_page=page, total_pages=total_pages, total_annonces=total_annonces,
        f_keyword=f_keyword, f_sort=f_sort, f_marques=f_marques, f_maisons=f_maisons,
        f_energies=f_energies, f_boites=f_boites,
        f_annee_min=f_annee_min, f_annee_max=f_annee_max,
        f_km_max=f_km_max, f_prix_max=f_prix_max, f_marge_min=f_marge_min,
        f_date_min=f_date_min, f_date_max=f_date_max
    )

@app.route('/reset_filters')
def reset_filters():
    session.pop('filters', None)
    return redirect(url_for('index'))

@app.route('/annonce/<int:annonce_id>')
def detail(annonce_id):
    conn = get_db()
    cursor = conn.cursor()
    query = "SELECT *, image_principale AS lien_image_vehicule, organisateur AS maison_vente FROM vue_details_ventes_avenir WHERE annonce_id = %s"
    cursor.execute(query, (annonce_id,))
    annonce = cursor.fetchone()
    
    if not annonce: return "Annonce introuvable", 404
    return render_template('detail.html', annonce=annonce)

if __name__ == '__main__':
    # init_view() n'est plus nécessaire car la vue est gérée dans Supabase
    app.run(debug=True, port=5001)