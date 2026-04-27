import sqlite3
import json

db_path = "D:\\Smart-Irrigation-Advisor\\smart_irrigation.db"

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
c = conn.cursor()

c.execute("SELECT * FROM irrigation_recommendations ORDER BY generated_at DESC LIMIT 5")
rows = c.fetchall()

for r in rows:
    print(dict(r))

conn.close()
