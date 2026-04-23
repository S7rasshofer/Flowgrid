import sqlite3, time, sys
path = sys.argv[1]
conn = sqlite3.connect(path)
conn.execute("create table if not exists t(x int)")
conn.commit()
time.sleep(25)
