import pandas as pd

# Lecture du CSV complet
df = pd.read_csv("../enterprise.csv")

# Prendre seulement les 10 premières lignes
df_head = df.head(10)

# Sauvegarder dans un nouveau fichier pour tests
df_head.to_csv("enterprise_test.csv", index=False)
print("Fichier test créé : enterprise_test.csv")
