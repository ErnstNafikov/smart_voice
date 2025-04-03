import sys
import os
import requests
# Добавляем родительскую папку в sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from settings import local_settings

def _ask(ftext,promt = 'You are a helpful assistant.'):
    API_KEY = local_settings.OPENROUTER_KEY
    API_URL = 'https://openrouter.ai/api/v1/chat/completions'

    headers = {
        'Authorization': f'Bearer {API_KEY}',
        'Content-Type': 'application/json'
    }
    data = {
        'model': 'deepseek/deepseek-chat:free',
        'messages': [{'role': 'system', 'content': promt},{'role': 'user', 'content': ftext}]
    }
    response = requests.post(API_URL, json=data, headers=headers)
    out_text = ''
    if response.status_code == 200:
        out_text = response.json()['choices'][0]['message']['content']
    else:
        out_text = 'Ошибка:' + response.status_code
    return out_text