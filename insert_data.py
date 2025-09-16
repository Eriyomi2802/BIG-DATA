import psycopg2
import random
import pandas as pd
from faker import Faker

# Configuration de la connexion PostgreSQL
conn = psycopg2.connect(
    dbname="DataMeteo",  # Remplacez par le nom de votre base de données
    user="aldiouma",  # Remplacez par votre nom d'utilisateur PostgreSQL
    password="mbaye",  # Remplacez par votre mot de passe PostgreSQL
    host="localhost",  # Assurez-vous que l'hôte est correct
    port="5432"  # Le port par défaut de PostgreSQL
)

# Créer un curseur pour exécuter des requêtes SQL
cur = conn.cursor()

# Initialisation du générateur de données fictives
fake = Faker()

# Liste des pays réels pour générer des pays aléatoires
countries = ['United States', 'China', 'India', 'Brazil', 'Russia', 'Japan', 'Germany', 'United Kingdom', 'France', 'Canada']

# Émissions de CO2 de base pour chaque pays (valeurs moyennes approximatives)
base_co2 = {
    'United States': 5000.0,
    'China': 8000.0,
    'India': 2200.0,
    'Brazil': 1500.0,
    'Russia': 4500.0,
    'Japan': 3500.0,
    'Germany': 3500.0,
    'United Kingdom': 3000.0,
    'France': 2500.0,
    'Canada': 2000.0
}

# Générer des données fictives pour insérer 200 lignes dans la table CO2
for _ in range(200):  # 200 itérations
    year = random.randint(1880, 2023)  # Générer un an aléatoire entre 1880 et 2023
    country = random.choice(countries)  # Choisir un pays aléatoire
    
    # Utilisation d'une variation créative sur la base des émissions de CO2
    base_emission = base_co2[country]
    co2 = round(base_emission + random.uniform(-200.0, 200.0) + (year - 1880) * 0.5, 3)  # Variation des émissions
    
    # CO2 par habitant, avec variation en fonction de la population
    population = random.randint(1_000_000_000, 1_500_000_000) if country in ['China', 'India'] else random.randint(30_000_000, 100_000_000)
    co2_per_capita = round(co2 / population * 1000, 3)  # Emissions par habitant
    
    # PIB avec une légère augmentation au fil des années et variation entre les pays
    gdp = round(random.uniform(0.0, 2.0) + (year - 1880) * 0.1, 2)  # Le PIB augmente chaque année
    
    # Variation des émissions selon les sources (ciment, charbon, pétrole) pour chaque pays
    if country == 'United States':
        cement_co2 = round(random.uniform(0.0, 20.0), 3)
        coal_co2 = round(random.uniform(300.0, 400.0), 3)
        oil_co2 = round(random.uniform(25.0, 40.0), 3)
    elif country == 'China':
        cement_co2 = round(random.uniform(50.0, 100.0), 3)
        coal_co2 = round(random.uniform(500.0, 600.0), 3)
        oil_co2 = round(random.uniform(10.0, 20.0), 3)
    elif country == 'India':
        cement_co2 = round(random.uniform(30.0, 60.0), 3)
        coal_co2 = round(random.uniform(300.0, 500.0), 3)
        oil_co2 = round(random.uniform(15.0, 25.0), 3)
    elif country == 'Brazil':
        cement_co2 = round(random.uniform(10.0, 30.0), 3)
        coal_co2 = round(random.uniform(50.0, 100.0), 3)
        oil_co2 = round(random.uniform(5.0, 15.0), 3)
    elif country == 'Russia':
        cement_co2 = round(random.uniform(20.0, 40.0), 3)
        coal_co2 = round(random.uniform(400.0, 500.0), 3)
        oil_co2 = round(random.uniform(20.0, 30.0), 3)
    elif country == 'Japan':
        cement_co2 = round(random.uniform(15.0, 30.0), 3)
        coal_co2 = round(random.uniform(100.0, 200.0), 3)
        oil_co2 = round(random.uniform(10.0, 15.0), 3)
    elif country == 'Germany':
        cement_co2 = round(random.uniform(10.0, 30.0), 3)
        coal_co2 = round(random.uniform(150.0, 250.0), 3)
        oil_co2 = round(random.uniform(10.0, 20.0), 3)
    elif country == 'United Kingdom':
        cement_co2 = round(random.uniform(5.0, 15.0), 3)
        coal_co2 = round(random.uniform(50.0, 100.0), 3)
        oil_co2 = round(random.uniform(5.0, 15.0), 3)
    elif country == 'France':
        cement_co2 = round(random.uniform(10.0, 20.0), 3)
        coal_co2 = round(random.uniform(100.0, 200.0), 3)
        oil_co2 = round(random.uniform(10.0, 20.0), 3)
    elif country == 'Canada':
        cement_co2 = round(random.uniform(5.0, 15.0), 3)
        coal_co2 = round(random.uniform(100.0, 200.0), 3)
        oil_co2 = round(random.uniform(20.0, 30.0), 3)
    
    # Calcul du share_global_co2 (émissions mondiales, qui peuvent augmenter en fonction des années)
    share_global_co2 = round(random.uniform(10.0, 15.0) + (year - 1880) * 0.1, 3)  # Valeur augmentant au fil des années

    # Insertion des données dans la table CO2
    cur.execute(""" 
        INSERT INTO CO2 (year, country, co2, co2_per_capita, population, gdp, cement_co2, coal_co2, oil_co2, share_global_co2)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (year, country, co2, co2_per_capita, population, gdp, cement_co2, coal_co2, oil_co2, share_global_co2))

# Committer les transactions pour les enregistrer dans la base de données
conn.commit()

# Fermer le curseur et la connexion
cur.close()
conn.close()

print("200 lignes de données fictives créatives insérées avec succès !")
