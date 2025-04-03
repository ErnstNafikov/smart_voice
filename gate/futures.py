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

PROD_NET = 'https://api.gateio.ws'
TEST_NET = 'https://fx-api-testnet.gateio.ws'
prefix = '/api/v4'

host = TEST_NET
headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

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

def _candlesticks(future_id,interval,dt_start,dt_end):
    url = '/futures/usdt/candlesticks'
    query_param = f'contract={future_id}&interval={interval}&from={dt_start}&to={dt_end}'
    r = requests.request('GET', host + prefix + url + "?" + query_param, headers=headers)
    return r.json()

def _positions():
    url = '/futures/usdt/positions'
    query_param = ''
    sign_headers = gen_sign('GET', prefix + url, query_param)
    headers.update(sign_headers)
    r = requests.request('GET', host + prefix + url, headers=headers)
    return r.json()

def _account():
    url = '/futures/usdt/account_book'
    query_param = ''
    sign_headers = gen_sign('GET', prefix + url, query_param)
    headers.update(sign_headers)
    r = requests.request('GET', host + prefix + url, headers=headers)
    return r.json()

def _send_order(future_id,size):    
    url = '/futures/usdt/orders'
    query_param = ''
    body= {"contract":future_id,"size":size,"price":"0","tif":"ioc"}
    body = json.dumps(body)
    print(body)
    sign_headers = gen_sign('POST', prefix + url, query_param, body)
    headers.update(sign_headers)
    r = requests.request('POST', host + prefix + url, headers=headers, data=body)
    return r.json()

def _cancel_all(future_id):
    url = '/futures/usdt/orders'
    query_param = f'contract={future_id}'
    sign_headers = gen_sign('DELETE', prefix + url, query_param)
    headers.update(sign_headers)
    r = requests.request('DELETE', host + prefix + url + "?" + query_param, headers=headers)
    return r.json()

def _close_order(future_id):    
    url = '/futures/usdt/orders'
    query_param = ''
    body= {"contract":future_id,"size":0,"price":"0","close":"true","tif":"ioc"}
    body = json.dumps(body)
    print(body)
    sign_headers = gen_sign('POST', prefix + url, query_param, body)
    headers.update(sign_headers)
    r = requests.request('POST', host + prefix + url, headers=headers, data=body)
    return r.json()