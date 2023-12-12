import sqlalchemy as sq
from sqlalchemy.orm import sessionmaker
from aws_postgres_conn import DBConnector
from models import create_tables, Publisher, Shop, Book, Stock, Sale
import json


def data_load(dsn: str):
    with open('test_data.json') as f:
        data = json.load(f)

    engine = sq.create_engine(dsn)
    create_tables(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

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

    # Task 3 - fetch sales data by publisher
    publishers = session.query(Publisher).all()
    print('\nPlease choose the id of your publisher:')
    pub_ids = []
    for pub in publishers:
        pub_ids.append(pub.id)
        print('id:', '|', pub.id, '|', pub.name)

    while True:
        try:
            choose_id = int(input('\nInsert id: '))
            if choose_id in pub_ids:
                pub_search = session.query(Publisher.name).filter(Publisher.id == choose_id).first()
                print('\n', 'You have chosen the publisher: ', pub_search.name, '\n')
                break
            else:
                print('The chosen id is out of available range please try again within available scope.')
        except ValueError:
            print('Please insert a number, not text. Try again.')

    sale_value = session.query(Sale.id, (Sale.price*Sale.count).label('sale_amount')).subquery()

    query = (session.query(Book.title, Shop.name, sale_value.c.sale_amount, Sale.date_sale).join(Publisher).join(Stock).
             join(Sale).join(Shop).join(sale_value, Sale.id == sale_value.c.id).filter(Publisher.id == choose_id).all())

    for q in query:
        title, shop, sale, date = q
        date = date.strftime('%d-%m-%Y')
        q = f'{title} | {shop} | {sale} | {date}'
        print(q)

    session.close()


create_connection = DBConnector('sens.txt', 'localhost', 5432, 'ubuntu', 22,
                                'postgres', 'netology_sqlalchemy')
tunnel = create_connection.connection()

DSN = (f"postgresql://{create_connection.database_user}:{create_connection.postgres_password}@localhost:"
       f"{tunnel.local_bind_port}/{create_connection.database}")

data_load(DSN)
