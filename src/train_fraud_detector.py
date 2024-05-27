#### ---- LIBRAIRIES

import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.metrics import accuracy_score, confusion_matrix, precision_score, recall_score, f1_score, roc_auc_score

import pickle

import matplotlib.pyplot as plt
import seaborn as sns

#### ---- PARAMETRES

# Fraction d'échantillonnage appliquée à la lecture des données
FRAC_RATIO = 0.2

# Fraction appliquée pour le jeu de données test
TEST_RATIO = 0.2

#### ---- TRAITEMENTS

# Lire le fichier de données
data = pd.read_parquet(
    "../data/transactions.parquet"
)

# Ne sélectionner qu'une fraction du jeu de données
data = data.sample(frac=FRAC_RATIO, random_state=42)

# Séparer les variables explicatives de la variable expliquée
X = data[["oldbalanceOrg","oldbalanceDest","type","amount"]]
y = data["isFraud"]

# Encoder les variables catégorielles
categorical_columns = X.select_dtypes(include=['object']).columns
categories = [X[col].unique() for col in categorical_columns]
preprocessor = ColumnTransformer(
    transformers=[
        ('cat', OneHotEncoder(categories=categories,drop='first',handle_unknown='ignore'), categorical_columns)
    ],
    remainder='passthrough'  # Garde les colonnes non catégorielles inchangées
)
X_encoded = preprocessor.fit_transform(X)

# Diviser les données en ensembles d'entraînement et de test
X_train, X_test, y_train, y_test = train_test_split(X_encoded, y, test_size=TEST_RATIO, random_state=42)

# Initialiser le modèle de régression logistique
logistic_model = LogisticRegression()

# Entraîner le modèle sur l'ensemble d'entraînement
logistic_model.fit(X_train, y_train)

# Réaliser la prévision sur les données test
y_pred = logistic_model.predict(X_test)

# Évaluer le modèle sur l'ensemble de test

# ... accuracy
accuracy = accuracy_score(y_test, y_pred)
print("Accuracy:", accuracy)

# ... matrice de confusion
conf_matrix = confusion_matrix(y_test, y_pred)
print("Matrice de confusion:")
print(conf_matrix)

# ... plot de la matrice de confusion
plt.figure(figsize=(8, 6))
sns.heatmap(conf_matrix, annot=True, fmt="d", cmap="Blues", cbar=False)
plt.title("Matrice de confusion")
plt.xlabel("Prédictions")
plt.ylabel("Valeurs réelles")
plt.savefig("../docs/images/confusion_matrix.png")
plt.show()


# ... precision
precision = precision_score(y_test, y_pred)
print("Precision:", precision)

# ... recall
recall = recall_score(y_test, y_pred)
print("Recall:", recall)

# ... F1-score
f1 = f1_score(y_test, y_pred)
print("F1-score:", f1)

# ... AUC-ROC
auc_roc = roc_auc_score(y_test, y_pred)
print("AUC-ROC:", auc_roc)

# Export le preprocesseur
with open('../data/preprocessor.pkl', 'wb') as f:
    pickle.dump(preprocessor, f)

# Exporter le modèle
with open('../data/modele_logistique.pkl', 'wb') as f:
    pickle.dump(logistic_model, f)
