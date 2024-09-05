import sqlite3
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect('health_events_data.db')

# Load the data into a pandas DataFrame
df = pd.read_sql_query("SELECT * FROM health_events", conn)

# Close the connection to the original database
conn.close()

# Handle missing values
# Fill missing numerical values with the mean of the column
numeric_columns = ['Affected_Population', 'Cost_of_Damages']
for col in numeric_columns:
    df[col] = df[col].fillna(df[col].mean())

# Fill missing categorical values with the mode of the column
categorical_columns = ['Condition', 'Agent', 'Reporting_Agency', 'City', 'Outcome']
for col in categorical_columns:
    df[col] = df[col].fillna(df[col].mode()[0])

# Remove any duplicate rows
df = df.drop_duplicates()

# Standardize the formatting of text columns
text_columns = ['Condition', 'Agent', 'Reporting_Agency', 'City', 'Outcome']
for col in text_columns:
    df[col] = df[col].str.strip().str.title()


df['Condition'] = df['Condition'].replace('Zombi Virus', 'Zombie Virus')

# Save the cleaned data back into a new table in the SQLite database
cleaned_conn = sqlite3.connect('cleaned_data.db')

# Save the cleaned data into the new database
df.to_sql('cleaned_health_events', cleaned_conn, if_exists='replace', index=False)

# Close the connection to the cleaned database
cleaned_conn.close()

print("Data cleaning completed. Cleaned data saved to 'cleaned_data' table.")