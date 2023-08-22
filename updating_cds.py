import sqlite3

connection = sqlite3.connect('moving_images_data/migration_database.db')

cur = connection.cursor()
cur.execute("SELECT recid FROM migration_state WHERE uploaded = 1")
recid_successes = [row[0] for row in cur.fetchall()]

with open('moving_images_data/update_cds', 'w') as f:
    for recid in recid_successes:
        correction_string = '<record>\n<controlfield tag="001">' + str(recid) + '</controlfield>\n<datafield tag="980" ind1="C" ind2=" ">\n<subfield code="b">migration_2023</subfield></datafield>\n</record>\n--correct\n\n'
        f.write(correction_string)