import sqlalchemy as sq
from sqlalchemy.orm import sessionmaker
from aws_postgres_conn import DBConnector
from models import Publisher, Shop, Book, Stock, Sale
from models import create_tables
import json

def data_load():
    with open('test_data.json') as f:
        data = json.load(f)

    for record in data:
        model = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale,
        }[record.get('model')]
        session.add(model(id=record.get('pk'), **record.get('fields')))
    session.commit()


create_connection = DBConnector('sens.txt', 'localhost', 5432, 'ubuntu', 22,
                                'postgres', 'netology_sqlalchemy')
tunnel = create_connection.connection()

DSN = (f"postgresql://{create_connection.database_user}:{create_connection.postgres_password}@localhost:"
       f"{tunnel.local_bind_port}/{create_connection.database}")

engine = sq.create_engine(DSN)

create_tables(engine)
Session = sessionmaker(bind=engine)
session = Session()

data_load()

session.close()
