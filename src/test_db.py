# save as test_db.py and run inside your virtualenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

conn_str = "mssql+pyodbc://BIuser:YOUR_PASSWORD@server/database?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(conn_str, connect_args={"connect_timeout": 5})

try:
    with engine.connect() as conn:
        print(conn.execute("SELECT 1").scalar())
    print("Connection OK")
except SQLAlchemyError as e:
    print("Connection failed:", e)
    