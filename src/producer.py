import time
import json
import pandas as pd

from kafka import KafkaProducer

SERVERS = [
    "54.154.1.106:9092",
    "3.250.162.154:9092",
    "54.216.118.46:9092"
]

# Créer un producer
producer = KafkaProducer(
    bootstrap_servers=SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('UTF-8')  # On encode en JSON puis en byte array
)

# Lire le fichier de données
data = pd.read_parquet(
    "./data/transactions.parquet"
)

# Déterminer le nombre de références différentes dans les données
num_data_steps = max(data["step"])
current_step = 1

# Exécuter à l'infini
while True:
    print("Current step", current_step)

    # ... ne sélectionner que les lignes de la référence courante
    sample = data[data["step"] == current_step]

    # ... ne prendre au maximum que 300 transactions
    if sample.shape[0] > 300:
        sample = sample.sample(n=300)

    # ... définier le nombre de batches et leur taille
    n_batches = round(sample.shape[0] / 20)
    if n_batches == 0:
        n_batches = 1
    rows_per_batch = int(sample.shape[0] / n_batches)

    # ... boucler sur les batchs et envoyer itérativement chacune de leurs lignes à Kafka
    for i in range(n_batches):
        for j in range(rows_per_batch):
            producer.send("transactions", sample.iloc[i * rows_per_batch + j].to_dict())
        time.sleep(1 / n_batches)

    # ... incrémenter la référence courante
    current_step = (current_step + 1) % num_data_steps