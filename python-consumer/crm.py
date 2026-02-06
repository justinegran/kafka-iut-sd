from kafka import KafkaConsumer
import json
import mysql.connector # Nécessite: pip install mysql-connector-python


create_table = """
CREATE TABLE IF NOT EXISTS Clients (
    SourceId INT NOT NULL,
    nom VARCHAR(100),
    prenom VARCHAR(100),
    email VARCHAR(255),
    PRIMARY KEY (SourceId)
);
"""

# --- 1. CONFIGURATION MARIADB ---
try:
    mariadb_connection = mysql.connector.connect(
        host='mariadb.gran4u-dev.svc.cluster.local',          
        user='crmuser',              
        password='LXX8gXdXb66bSlPp',  
        database='crm'  
    )
    cursor = mariadb_connection.cursor()
    print(" Connecté à MariaDB avec succès.")

       # --- CRÉER LA TABLE SI ELLE N'EXISTE PAS ---
    cursor.execute(create_table)
    mariadb_connection.commit()

except mysql.connector.Error as err:
    print(f" Erreur critique connexion MariaDB: {err}")
    exit(1)

# Requête pour Insérer ou Mettre à jour (UPSERT)
# Si le SourceId existe déjà, on met à jour les champs, sinon on crée.




sql_upsert = """
    INSERT INTO Clients (SourceId, nom, prenom, email) 
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
    nom = VALUES(nom), 
    prenom = VALUES(prenom), 
    email = VALUES(email)
"""

# Requête pour Supprimer
sql_delete = "DELETE FROM Clients WHERE SourceId = %s"


# --- 2. CONFIGURATION KAFKA ---
broker = 'my-kafka.gran4u-dev.svc.cluster.local:9092'
topic = 'dbserver1.inventory.customers'

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[broker],
    sasl_mechanism='SCRAM-SHA-256',
    security_protocol='SASL_PLAINTEXT',
    sasl_plain_username='user1',
    sasl_plain_password='s3qiduaFJS',  
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='crm6'
)

print(f"Listening to topic {topic}")

# --- 3. BOUCLE DE TRAITEMENT ---
for message in consumer:
    if message.value:
        try:
            # 1. Décoder le JSON global
            data = json.loads(message.value.decode('utf-8'))
            
            # 2. Accéder à l'objet 'payload'
            payload = data.get('payload', data) 

            # Vérification de sécurité si payload est vide (ex: tombstone message)
            if payload:
                # 3. Récupérer les états 'before' et 'after'
                before_data = payload.get('before')
                after_data = payload.get('after')
                operation = payload.get('op')  # 'c', 'u', 'd', 'r'

                print("--- Nouveau Changement Détecté ---")
                print(f"Opération: {operation}")

                # --- CAS 1 : INSERTION / LECTURE / UPDATE (c, r, u) ---
                # Dans ces cas, on veut que les données dans MariaDB correspondent à 'after_data'
                if operation in ['c', 'r', 'u']:
                    if after_data:
                        # Mapping des champs (Source -> Destination)
                        s_id = after_data.get('id')          # SourceId
                        s_nom = after_data.get('last_name')  # nom
                        s_prenom = after_data.get('first_name') # prénom
                        s_email = after_data.get('email')    # email

                        # Exécution SQL

                        cursor.execute(sql_upsert, (s_id, s_nom, s_prenom, s_email))
                        mariadb_connection.commit()
                        
                        if operation == 'u':
                            print(f"Client mis à jour : {s_prenom} {s_nom}")
                        else:
                            print(f"Client créé/synchronisé : {s_prenom} {s_nom}")

                # --- CAS 2 : SUPPRESSION (d) ---
                elif operation == 'd':
                    if before_data:
                        s_id = before_data.get('id')
                        
                        # Exécution SQL
                        cursor.execute(sql_delete, (s_id,))
                        mariadb_connection.commit()
                        print(f"Client supprimé (ID Source: {s_id})")

        except json.JSONDecodeError:
            print("Erreur de décodage JSON")
        except mysql.connector.Error as err:
            print(f"Erreur SQL lors du traitement : {err}")
        except AttributeError:
            print("Format de message inattendu")