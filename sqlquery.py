import psycopg2

def connection():
    try:
        conn = psycopg2.connect(dbname='please', user='annikov', password='12345678', host='localhost',
                                port=5432)
        print("connection is successful")
        return conn
    except:
        print('connection not established')

def get_all_db(conn):
    cur=conn.cursor()
    cur.execute("SELECT owl.owner_id,owl.location_id,cst.card_number,cst.average_check,cst.current_balance,cst.credit,cst.number_of_cards,"
                "crd.has_credit_card,crd.bank,crd.offer_type,"
                "l.city,l.country,"
                "own.gender,own.education,own.contact_type,own.response_to_campaign"
                " FROM owner_locations owl "
                " INNER JOIN card_statistics cst ON owl.owner_id=cst.owner_id"
                " INNER JOIN locations l ON owl.location_id=l.location_id "
                " INNER JOIN cards crd ON owl.owner_id=crd.owner_id "
                " INNER JOIN owners own ON owl.owner_id=own.owner_id LIMIT 50;" ) #здесь мы пишем запрос в базу данных
    pleases=cur.fetchall()
    return pleases


if __name__== '__main__':
    conn=connection()
    templ=get_all_db(conn)
    for i in templ:
        print(i)
