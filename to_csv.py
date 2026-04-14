import pandas as pd

# Load the Excel file
excel_file = 'excel_demarchage.xlsx'
all_sheets = pd.read_excel(excel_file, sheet_name=None)

# Loop through the dictionary and save each sheet
for sheet_name, data in all_sheets.items():
    data.to_csv(f"{sheet_name}.csv", index=False)