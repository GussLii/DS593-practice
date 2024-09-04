import csv
import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('health_events_data.db')
cursor = conn.cursor()

# Create a table with the appropriate schema
cursor.execute('''
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT,
    description TEXT,
    location TEXT,
    date TEXT,
    reported_cases INTEGER
)
''')

# Load and insert data from the CSV file
with open('funny_epidemiological_events.csv', 'r') as file:
    csv_reader = csv.DictReader(file)
    to_db = [(i['event_type'], i['description'], i['location'], i['date'], i['reported_cases']) for i in csv_reader]
    cursor.executemany('''
    INSERT INTO events (event_type, description, location, date, reported_cases) 
    VALUES (?, ?, ?, ?, ?);
    ''', to_db)
    conn.commit()

# Close the database connection
conn.close()
