import pandas as pd

# 1. Read the original CSV file
file_path = "1. Démarchage contacté - Démarcharge SDC 2026.csv"
df = pd.read_csv(file_path)

# 2. Merge 'mail' and 'Mail' into a single 'Email' column
# combine_first fills missing values in 'mail' with values from 'Mail'
df['Email'] = df['mail'].combine_first(df['Mail'])

# 3. Create a unified 'Nom du Contact' (Contact Name) column
df['Nom du Contact'] = df['Prénom'].fillna('') + ' ' + df['Nom'].fillna('')
df['Nom du Contact'] = df['Nom du Contact'].str.strip() # Remove extra spaces if one name is missing

# 4. Add the 'Motif de refus' (Reason of refusal) column
df['Motif de refus'] = ''

# 5. Clean up redundant columns that we just merged
df = df.drop(columns=['mail', 'Mail', 'Prénom', 'Nom'], errors='ignore')

# 6. Reorganize columns to prioritize crucial tracking information at the beginning
cols = [
    'Château / Entreprise', 'Nom du Contact', 'Email', 'Téléphone', 
    'Statut de la demande', 'Motif de refus', 'Membre de SDC', 'Catégorie', 
    'Apparaît dans la bible ? ', 'type de vins princiapl', 'Pôle concerné', 
    'Région', 'Poste', 'Nombre de bouteilles', 'Logistique', 'Suivi des étapes', 
    'Contexte de la demande/commentaires', 'Région/Appellation', 'Aspect RSE', 
    'Possible de démarcher à nouveau ?', 'Si oui, quel type ?'
]

# Ensure we don't lose any other columns if there were extra ones
remaining_cols = [col for col in df.columns if col not in cols]
final_cols = cols + remaining_cols

# Apply the new column order
df = df[final_cols]

# 7. Save the improved dataframe to a new CSV file
output_path = "Improved_Tracking_SDC_2026.csv"
df.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"File successfully cleaned and saved to: {output_path}")