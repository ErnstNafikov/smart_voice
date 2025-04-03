import sys
import os
import time
import json
# Добавляем родительскую папку в sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from sqlalchemy import create_engine, Table, MetaData, insert, update, select, bindparam, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
import pandas as pd
from datetime import datetime, timedelta
from settings import local_settings,promt
from gate import futures
from ai import deepseek

def _trader_set():
    engine = create_engine(f'postgresql://{local_settings.POSTGRES_USER}:{local_settings.POSTGRES_PASSWORD}@{local_settings.POSTGRES_HOST}:{local_settings.POSTGRES_PORT}/{local_settings.POSTGRES_DB}')
    h_tickers_el = []
    positions = None
    balance = None
    df = None
    upd_time = datetime.fromtimestamp(time.time())
    with engine.connect() as conn:
        metadata = MetaData()
        h_tickers_table = Table('h_tickers', metadata, autoload_with=engine)
        four_weeks_ago = datetime.now() - timedelta(weeks=4)
        print(four_weeks_ago)
        # Формируем запрос с фильтром по дате
        query = select(h_tickers_table).where(
            h_tickers_table.c.dt >= four_weeks_ago,  # За последние 4 недели
            h_tickers_table.c.future_id == 'BTC_USDT'  # Только для BTC_USDT
        )
        # Выполняем запрос
        h_tickers = conn.execute(query).fetchall()
        h_tickers_el = [
            {key: value for key, value in row._mapping.items() if key != 'tm_id'}
            for row in h_tickers
        ]
        
        df = pd.read_sql(query, conn)
        # Предположим, что у нас есть колонка 'price'
        df['SMA_20'] = df['c'].rolling(window=20).mean()
        
        gate_value_table = Table('gate_value', metadata, autoload_with=engine)
        # Выполняем запрос и преобразуем результаты
        gate_value = conn.execute(select(gate_value_table)).fetchall()
        for row in gate_value:
            positions = dict(row._mapping)
        gate_user_table = Table('gate_user', metadata, autoload_with=engine)
        # Выполняем запрос и преобразуем результаты
        gate_user = conn.execute(select(gate_user_table)).fetchall()
        for row in gate_user:
            balance = row
    print(df)
    # trader_plus_input = {
        # 'time':upd_time,
        # 'candles':h_tickers_el,
        # 'position':positions,
        # 'balance':balance
    # }
    # trader_plus_answer = deepseek._ask(str(trader_plus_input),promt.promt_trader_plus)
    
    analys_input = {
        'time':upd_time,
        'candles':h_tickers_el
    }
    #analys_answer = deepseek._ask(str(analys_input),promt.promt_analys)
    #print(trader_plus_answer)
    # with engine.connect() as conn:
        # metadata = MetaData()
        # log_worker_table = Table('log_worker', metadata, autoload_with=engine)
        # # i_log_worker = insert(log_worker_table).values(dt = upd_time, id='trader_plus', input=str(trader_plus_input),output = trader_plus_answer)
        # # result = conn.execute(i_log_worker)
        # i_log_worker = insert(log_worker_table).values(dt = upd_time, id='analys', input=str(analys_input),output = analys_answer)
        # result = conn.execute(i_log_worker)
        # conn.commit()  # Фиксируем изменения
    
    # try:
        # res = ''
        # trader_plus_data = json.loads(trader_plus_answer)  # Преобразуем строку в словарь Python
        # if trader_plus_data["type"] == 'close':
            # res = futures._close_position('BTC_USDT')
        # if trader_plus_data["type"] == 'change':
            # res = futures._send_order('BTC_USDT',trader_plus_data["lots"])
        # if trader_plus_data["type"] == 'open':
            # try:
                # price = int(trader_plus_data["price"])
                # if price > 0:
                    # res = futures._send_order_v2('BTC_USDT',trader_plus_data["lots"],price)
            # except:
                # print('Ошибка поля price',trader_plus_answer)
                # res = futures._send_order('BTC_USDT',trader_plus_data["lots"])
        
        # with engine.connect() as conn:
            # metadata = MetaData()
            # log_orders_table = Table('log_orders', metadata, autoload_with=engine)
            # i_log_order = insert(log_orders_table).values(dt = upd_time, data=str(res))
            # result = conn.execute(i_log_order)
            # conn.commit()  # Фиксируем изменения
    # except:
        # print('Ошибка преобразования',trader_plus_answer)
