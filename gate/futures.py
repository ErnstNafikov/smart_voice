import sys
import os
import requests
import datetime
import time
import hashlib
import hmac
import json
# Добавляем родительскую папку в sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from settings import local_settings
from sqlalchemy import create_engine, Table, MetaData, insert, select, bindparam
from sqlalchemy.dialects.postgresql import insert as pg_insert

def gen_sign(method, url, query_string=None, payload_string=None):
    key = local_settings.GATE_KEY
    secret = local_settings.GATE_SECRET
    t = time.time()
    m = hashlib.sha512()
    m.update((payload_string or '').encode('utf-8'))
    hashed_payload = m.hexdigest()
    s = '%s\n%s\n%s\n%s\n%s' % (method, url, query_string or '', hashed_payload, t)
    sign = hmac.new(secret.encode('utf-8'), s.encode('utf-8'), hashlib.sha512).hexdigest()
    return {'KEY': key, 'Timestamp': str(t), 'SIGN': sign}

engine = create_engine(f'postgresql://{local_settings.POSTGRES_USER}:{local_settings.POSTGRES_PASSWORD}@{local_settings.POSTGRES_HOST}:{local_settings.POSTGRES_PORT}/{local_settings.POSTGRES_DB}')
PROD_NET = 'https://api.gateio.ws/api/v4'
TEST_NET = 'https://fx-api-testnet.gateio.ws/api/v4'
with engine.connect() as conn:
    metadata = MetaData()
    conf_value_table = Table('conf_value', metadata, autoload_with=engine)
    sa_tickers_table = Table('sa_tickers', metadata, autoload_with=engine)
    h_tickers_table = Table('h_tickers', metadata, autoload_with=engine)
    conf_value_data = conn.execute(select(conf_value_table)).fetchall()
    for conf_value_el in conf_value_data:
        conf_value_dict = dict(conf_value_el._asdict())
        future_id = conf_value_dict['future_id']
        dt_start = int(conf_value_dict['dt_hist'].timestamp())
        dt_end = dt_start + 3600*1
        print(future_id,dt_start,dt_end)
        URL = f'{TEST_NET}/futures/usdt/candlesticks'
        query_param = f'contract={future_id}&interval=1h&from={dt_start}&to={dt_end}'
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        body = {'contract': 'BTC_USD', 'size': 100, 'price': '30', 'tif': 'gtc'}
        data = json.dumps(body)
        sign_headers = gen_sign('GET', URL + '?' + query_param, query_param)
        sign_headers.update(headers)
        res = requests.get(URL + '?' + query_param, headers=sign_headers)
        data = res.json()
        #print(data)
        conn.execute(sa_tickers_table.delete())  # Удаляем все строки
        conn.commit()
        for ins_row in data:
            i_tickers = insert(sa_tickers_table).values(future_id = future_id, dt=datetime.datetime.fromtimestamp(ins_row['t']), tm_id=ins_row['t'],o = ins_row['o'],c = ins_row['c'],v = ins_row['v'],l = ins_row['l'],h = ins_row['h'])
            result = conn.execute(i_tickers)
        conn.commit()  # Фиксируем изменения
        
        sa_tickers_data = conn.execute(select(sa_tickers_table)).fetchall()
        for sa_tickers_el in sa_tickers_data:
            sa_tickers_dict = dict(sa_tickers_el._asdict())
            # Автоматический MERGE (ON CONFLICT DO UPDATE)
            # Генерация SQL-запроса с on_conflict_do_update
            i_tickers = pg_insert(h_tickers_table).values(**sa_tickers_dict)
            u_tickers = i_tickers.on_conflict_do_update(
                index_elements=['tm_id'],
                set_={col.key: col for col in i_tickers.excluded if col.key != 'tm_id'}
            )
            print(u_tickers)
            conn.execute(u_tickers)
        conn.commit()  # Подтверждаем изменения