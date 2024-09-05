import sqlite3
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect('health_events_data.db')

# Load the data into a pandas DataFrame
df = pd.read_sql_query("SELECT * FROM health_events", conn)

# Calculate number of missing cells by column
missing_cells = df.isnull().sum()
print("Number of missing cells by column:")
print(missing_cells)
print("\n")

# Calculate number of categories in specified columns
columns_to_check = ['Condition', 'Agent', 'Reporting_Agency', 'City']
for column in columns_to_check:
    unique_categories = df[column].nunique()
    #print(f"Number of categories in {column}: {unique_categories}")
    print("number of categories in",column,":",unique_categories)

# Close the database connection
conn.close()