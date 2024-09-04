import csv
import sqlite3
import re

def clean_number(value):
    if value is None or value.strip() == '':
        return None
    return re.sub(r'[^\d.]', '', value)

# Connect to the SQLite database
conn = sqlite3.connect('health_events_data.db')
cursor = conn.cursor()

# Drop the existing table if it exists
cursor.execute('DROP TABLE IF EXISTS health_events')

# Create the table (adjusted schema based on your CSV structure)
cursor.execute('''CREATE TABLE IF NOT EXISTS health_events
                  (Event_ID INTEGER,
                   Condition TEXT,
                   Agent TEXT,
                   Reporting_Agency TEXT,
                   Affected_Population REAL,
                   City TEXT,
                   Event_Start_Date TEXT,
                   Event_End_Date TEXT,
                   Outcome TEXT,
                   Cost_of_Damages REAL)''')

# Read the CSV and insert data into the database
with open('funny_epidemiological_events.csv', 'r') as csvfile:
    csv_reader = csv.DictReader(csvfile)
    for row in csv_reader:
        cursor.execute('''INSERT INTO health_events 
                          (Event_ID, Condition, Agent, Reporting_Agency, Affected_Population,
                           City, Event_Start_Date, Event_End_Date, Outcome, Cost_of_Damages)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                       (clean_number(row['Event ID']) if row['Event ID'] else None,
                        row['Condition'],
                        row['Agent'], 
                        row['Reporting Agency'],
                        clean_number(row['Affected Population']),
                        row['City'],
                        row['Event Start Date'],
                        row['Event End Date'],
                        row['Outcome'],
                        clean_number(row['Cost of Damages ($)'])))

# Commit changes and close the connection
conn.commit()
conn.close()

print("Data import completed successfully.")