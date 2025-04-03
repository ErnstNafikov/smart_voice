import sys
import os
import time
import json
# Добавляем родительскую папку в sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from datetime import datetime
from sqlalchemy import create_engine, Table, MetaData, insert, update, select, bindparam, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from settings import local_settings
from gate import futures
#from worker import worker

def sa_tickers(future_name,interval):
    engine = create_engine(f'postgresql://{local_settings.POSTGRES_USER}:{local_settings.POSTGRES_PASSWORD}@{local_settings.POSTGRES_HOST}:{local_settings.POSTGRES_PORT}/{local_settings.POSTGRES_DB}')
    with engine.connect() as conn:
        metadata = MetaData()
        conf_value_table = Table('conf_value', metadata, autoload_with=engine)
        sa_tickers_table = Table('sa_tickers', metadata, autoload_with=engine)
        #h_tickers_table = Table('h_tickers', metadata, autoload_with=engine)
        conf_value_data = conn.execute(select(conf_value_table)).fetchall()
        for conf_value_el in conf_value_data:
            conf_value_dict = dict(conf_value_el._asdict())
            future_id = conf_value_dict['future_id']
            if future_name == future_id:
                dt_start = int(conf_value_dict['dt'].timestamp())
                dt_end = dt_start + 3600*1
                if dt_start < time.time() - 3600*2:
                    dt_end = dt_start + 3600*1000
                print(future_id,interval,dt_start,dt_end)
                data = futures._candlesticks(future_id,interval,dt_start,dt_end)
                #print(data)
                conn.execute(sa_tickers_table.delete())  # Удаляем все строки
                conn.commit()
                for ins_row in data:
                    i_tickers = insert(sa_tickers_table).values(future_id = future_id, interval = interval,dt=datetime.fromtimestamp(ins_row['t']), tm_id=ins_row['t'],o = ins_row['o'],c = ins_row['c'],v = ins_row['v'],l = ins_row['l'],h = ins_row['h'])
                    result = conn.execute(i_tickers)
                conn.commit()  # Фиксируем изменения

def h_tickers(future_name,interval):
    engine = create_engine(f'postgresql://{local_settings.POSTGRES_USER}:{local_settings.POSTGRES_PASSWORD}@{local_settings.POSTGRES_HOST}:{local_settings.POSTGRES_PORT}/{local_settings.POSTGRES_DB}')
    with engine.connect() as conn:
        metadata = MetaData()
        today = datetime.now()
        sa_tickers_table = Table('sa_tickers', metadata, autoload_with=engine)
        h_tickers_table = Table('h_tickers', metadata, autoload_with=engine)
        sa_tickers_data = conn.execute(select(sa_tickers_table).where(
            sa_tickers_table.c.interval == interval,
            sa_tickers_table.c.future_id == future_name,
            sa_tickers_table.c.dt < today
        )).fetchall()
        for sa_tickers_el in sa_tickers_data:
            sa_tickers_dict = dict(sa_tickers_el._asdict())
            # Автоматический MERGE (ON CONFLICT DO UPDATE)
            # Генерация SQL-запроса с on_conflict_do_update
            i_tickers = pg_insert(h_tickers_table).values(**sa_tickers_dict)
            u_tickers = i_tickers.on_conflict_do_update(
                index_elements=['future_id', 'interval', 'tm_id'],  # Указываем столбцы, по которым будет конфликт
                set_={col.key: col for col in i_tickers.excluded if col.key not in ('future_id', 'interval', 'tm_id')}
            )
            #print(u_tickers)
            conn.execute(u_tickers)
        conn.commit()  # Подтверждаем изменения

def upd_horizon(future_name):
    engine = create_engine(f'postgresql://{local_settings.POSTGRES_USER}:{local_settings.POSTGRES_PASSWORD}@{local_settings.POSTGRES_HOST}:{local_settings.POSTGRES_PORT}/{local_settings.POSTGRES_DB}')
    with engine.connect() as conn:
        metadata = MetaData()
        conf_value_table = Table('conf_value', metadata, autoload_with=engine)
        h_tickers_table = Table('h_tickers', metadata, autoload_with=engine)
        max_date = conn.execute(
            select(func.max(h_tickers_table.c.dt))
        ).scalar()
        print(max_date)
        upd_horizon = (
            update(conf_value_table)
            .where(conf_value_table.c.future_id == future_name)
            .values(dt=max_date)
        )
        conn.execute(upd_horizon)
        conn.commit()

def upd_positions(future_name):
    engine = create_engine(f'postgresql://{local_settings.POSTGRES_USER}:{local_settings.POSTGRES_PASSWORD}@{local_settings.POSTGRES_HOST}:{local_settings.POSTGRES_PORT}/{local_settings.POSTGRES_DB}')
    upd_time = datetime.fromtimestamp(time.time())
    with engine.connect() as conn:
        metadata = MetaData()
        gate_value_table = Table('gate_value', metadata, autoload_with=engine)
        conn.execute(gate_value_table.delete())  # Удаляем все строки
        data = futures._positions()
        #print(data)
        conn.commit()
        for ins_row in data:
            if future_name == ins_row['contract']:
                i_tickers = insert(gate_value_table).values(future_id = ins_row['contract'], size=ins_row['size'], entry_price=ins_row['entry_price'],value = ins_row['value'],unrealised_pnl = ins_row['unrealised_pnl'],open_time = datetime.fromtimestamp(ins_row['open_time']),update_time = upd_time)
                result = conn.execute(i_tickers)
        conn.commit()  # Фиксируем изменения

def upd_balance():
    engine = create_engine(f'postgresql://{local_settings.POSTGRES_USER}:{local_settings.POSTGRES_PASSWORD}@{local_settings.POSTGRES_HOST}:{local_settings.POSTGRES_PORT}/{local_settings.POSTGRES_DB}')
    upd_time = datetime.fromtimestamp(time.time())
    with engine.connect() as conn:
        metadata = MetaData()
        gate_user_table = Table('gate_user', metadata, autoload_with=engine)
        conn.execute(gate_user_table.delete())  # Удаляем все строки
        data = futures._account()
        #print(data)
        conn.commit()
        ins_row = data[0]
        i_tickers = insert(gate_user_table).values(balance = ins_row['balance'])
        result = conn.execute(i_tickers)
        conn.commit()  # Фиксируем изменения

if __name__ == '__main__':
    sa_tickers('BTC_USDT','1h')
    h_tickers('BTC_USDT','1h')
    upd_horizon('BTC_USDT')
    #futures._cancel_all('BTC_USDT')
    upd_positions('BTC_USDT')
    upd_balance()
    # worker._trader_set()
    # upd_positions('BTC_USDT')
    # upd_balance()