import sqlite3

connection = sqlite3.connect('moving_images_data/migration_database.db')

with open('schema.sql') as f:
    connection.executescript(f.read())

connection.commit()
connection.close()