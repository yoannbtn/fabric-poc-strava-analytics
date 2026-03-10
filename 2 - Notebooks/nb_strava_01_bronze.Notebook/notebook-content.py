# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a3ece0f3-09f5-4890-aa41-0a3f456d5502",
# META       "default_lakehouse_name": "lh_dp600_se",
# META       "default_lakehouse_workspace_id": "30f64b62-7548-4676-bc43-b5a2cd4af01d",
# META       "known_lakehouses": [
# META         {
# META           "id": "a3ece0f3-09f5-4890-aa41-0a3f456d5502"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_strava_01_bronze

# CELL ********************

import os
import json
import time
import requests
from datetime import datetime
from notebookutils import mssparkutils

# --- 1. PARAMÈTRES (Naming & Config) ---
LAKEHOUSE_NAME = "lh_dp600_se"
BRONZE_PATH_ACT = f"/lakehouse/default/Files/Bronze/Strava/Activities"
BRONZE_PATH_GEAR = f"/lakehouse/default/Files/Bronze/Strava/Gears"

# --- 2. RÉCUPÉRATION SÉCURISÉE DES SECRETS ---
VAULT_URL = "https://discovery-key-vault.vault.azure.net/"
CLIENT_ID = mssparkutils.credentials.getSecret(VAULT_URL, "strava-client-id")
CLIENT_SECRET = mssparkutils.credentials.getSecret(VAULT_URL, "strava-client-secret")
REFRESH_TOKEN = mssparkutils.credentials.getSecret(VAULT_URL, "strava-refresh-token")

# --- 2. FONCTIONS TECHNIQUES ---

def get_access_token(cid, csecret, rtoken):
    """Génère un nouveau bearer token valide."""
    url = "https://www.strava.com/oauth/token"
    payload = {
        'client_id': cid,
        'client_secret': csecret,
        'refresh_token': rtoken,
        'grant_type': 'refresh_token'
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json()['access_token']

def fetch_gear_snapshot(gear_id, header, timestamp):
    """Télécharge le détail du matériel avec un timestamp (Snapshot journalier)."""
    file_name = f"gear_{gear_id}_{timestamp}.json"
    file_path = os.path.join(BRONZE_PATH_GEAR, file_name)
    
    # Respect du quota : petite pause entre les appels de matériel
    time.sleep(0.5) 
    
    res_gear = requests.get(f"https://www.strava.com/api/v3/gear/{gear_id}", headers=header)
    
    if res_gear.status_code == 200:
        os.makedirs(BRONZE_PATH_GEAR, exist_ok=True)
        with open(file_path, "w", encoding='utf-8') as f:
            json.dump(res_gear.json(), f, ensure_ascii=False, indent=4)
    else:
        print(f"   /!\\ Erreur {res_gear.status_code} pour le gear {gear_id}")

# --- 3. EXÉCUTION PRINCIPALE ---

try:
    current_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"--- Ingestion Strava (Full History Mode) : {current_ts} ---")
    
    # Étape A : Authentification
    token = get_access_token(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
    header = {'Authorization': f"Bearer {token}"}
    
    # Étape B : Ingestion de TOUTES les Activités (Pagination)
    print("Extraction de l'historique des activités...")
    all_activities = []
    page = 1
    per_page = 100 # On maximise chaque appel pour minimiser le nombre de requêtes

    while True:
        print(f"   -> Récupération Page {page}...")
        params = {'per_page': per_page, 'page': page}
        res = requests.get("https://www.strava.com/api/v3/athlete/activities", headers=header, params=params)
        res.raise_for_status()
        
        data = res.json()
        if not data: # Fin de l'historique
            break
            
        all_activities.extend(data)
        page += 1
        time.sleep(0.5) # Pause de politesse pour l'API

    # Sauvegarde du fichier JSON (Full History)
    filename_act = f"activities_full_history_{current_ts}.json"
    full_path_act = os.path.join(BRONZE_PATH_ACT, filename_act)
    os.makedirs(BRONZE_PATH_ACT, exist_ok=True)
    
    with open(full_path_act, "w", encoding='utf-8') as f:
        json.dump(all_activities, f, ensure_ascii=False, indent=4)
    
    print(f"✅ OK : {len(all_activities)} activités sauvegardées en Bronze.")

    # Étape C : Snapshot du matériel (Gear)
    print("Scan exhaustif du matériel pour snapshot de distance...")
    
    # Identification des IDs uniques dans le flux que l'on vient de télécharger
    gears_found = {a['gear_id'] for a in all_activities if a.get('gear_id')}
    
    # Optionnel : Ajout des IDs déjà connus en Silver pour ne rien rater
    try:
        df_silver = spark.table("stg_strava_activities")
        gears_historical = {row.GearID for row in df_silver.select("GearID").distinct().collect() if row.GearID}
        gears_found = gears_found.union(gears_historical)
    except Exception:
        pass

    print(f"   -> {len(gears_found)} équipements identifiés. Début des snapshots...")
    
    for gid in gears_found:
        fetch_gear_snapshot(gid, header, current_ts)

    print(f"--- Ingestion Bronze terminée avec succès ---")

except Exception as e:
    print(f"ERREUR : {str(e)}")
    raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
