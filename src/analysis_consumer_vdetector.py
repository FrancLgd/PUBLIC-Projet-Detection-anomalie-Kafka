#### ---- LIBRAIRIES

import time
import json
import pandas as pd
from datetime import datetime
from kafka import KafkaConsumer
from collections import deque
import pickle
import numpy as np

#### ---- PARAMETRES

SERVERS = [
    "54.154.1.106:9092",
    "3.250.162.154:9092",
    "54.216.118.46:9092"
]

#### ---- TRAITEMENTS

def main():
        
    # Charger le préprocesseur
    with open('./data/preprocessor.pkl', 'rb') as f:
        preprocessor = pickle.load(f)

    # Charger le modèle prédictif
    with open('./data/modele_logistique.pkl', 'rb') as f:
        logistic_model = pickle.load(f)

    # Initialiser le consumer
    consumer = KafkaConsumer(
        'transactions',
        bootstrap_servers = SERVERS,
        group_id = 'regular',
        value_deserializer=lambda x: json.loads(x.decode('UTF-8'))
    )

    # Initialiser une liste tampon
    buffer_list = []

    # Boucle infini
    while True:

        # ... attendre une seconde
        time.sleep(1)

        # ... faire un poll des derniers records
        new_messages = consumer.poll(timeout_ms=1000)

        # ... incrémenter la liste tampon
        if new_messages:
            for tp, messages in new_messages.items():
                for msg in messages:
                    buffer_list.append({'timestamp':msg.timestamp / 1000,'value':msg.value})
        
        # ... calculer et afficher l'heure en cours
        now_time = time.time()
        print(datetime.fromtimestamp(now_time).strftime("%Y-%m-%d %H:%M:%S"))

        # ... filtrer la file tampon sur les 30 dernières secondes
        buffer_list = [el for el in buffer_list if el['timestamp'] > (now_time-30)]

        # ... calculer les statistiques demandées
        if buffer_list:

            res = {}

            res["Nombre total de transaction réalisées au cours des 30 dernières secondes"] = len(buffer_list)

            fraud_list = [el for el in buffer_list if el['value']['isFraud']]

            res["Nombre de transactions frauduleuses effectives"] = len(fraud_list)
            
            # ... appliquer le modèle de prédiction des fraudes
            df = pd.DataFrame([el["value"] for el in buffer_list])
            df = df[["oldbalanceOrg","oldbalanceDest","type","amount"]]
            df = preprocessor.fit_transform(df)
            predicted_frauds = logistic_model.predict(df)

            res["Nombre de transactions frauduleuses prévues"] = np.sum(predicted_frauds)

            res["Montant total des transactions (frauduleuses et non frauduleuses)"] = int(sum([el["value"]["amount"] for el in buffer_list]))
            
            if fraud_list:
                
                tot_montant_fraud = sum([el["value"]["amount"] for el in fraud_list])
            else:
                tot_montant_fraud = 0

            res["Part que représente le montant des transactions frauduleuses sur le montant total"] = \
                "{} %".\
                    format(int(100 * tot_montant_fraud / res["Montant total des transactions (frauduleuses et non frauduleuses)"]))
            
            print(res)

        else:
            print("Aucune transaction au cours des 30 dernières secondes.")

if __name__ == '__main__':
    main()