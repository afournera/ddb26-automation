import pandas as pd

filename = 'mail_list.csv'

# Load the CSV
df = pd.read_csv(filename)

# Select the 3rd column by index
df = df.iloc[:, [2]]

# Save it back to the same filename
df.to_csv(filename, index=False)