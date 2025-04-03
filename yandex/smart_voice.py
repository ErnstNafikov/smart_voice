import sys
import os
import requests
import re
# Добавляем родительскую папку в sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from settings import local_settings
from http.cookies import SimpleCookie

def _text_voice(scenario_id,scenario_name,ftext):
    cookie = SimpleCookie()
    cookie.load(local_settings.YANDEX_COOKIES)
    cookies = {k: v.value for k, v in cookie.items()}
    session = requests.session()
    raw = session.get('https://yandex.ru/quasar', cookies=cookies).text
    m = re.search('"csrfToken2":"(.+?)"', raw)
    csrf_token = m[1]
    headers = {
    'x-csrf-token': csrf_token
    }
    data = {
        'name': scenario_name,
        'icon': 'home',
        'triggers': [
            {
                'trigger': {
                    'type': 'scenario.trigger.voice',
                    'value': 'Повтори ' + scenario_name
                },
                'type': 'scenario.trigger.voice',
                'value': 'Повтори ' + scenario_name
            }
        ],
        'steps': [
            {
                'type': 'scenarios.steps.actions',
                'parameters': {
                    'launch_devices': [
                        {
                            'id': '3cffc62f-6d13-4b53-8cd8-0ce2f7a46bf2',
                            'capabilities': [
                                {
                                    'type': 'devices.capabilities.quasar',
                                    'state': {
                                        'instance': 'tts',
                                        'value': {
                                            'text': ftext
                                        }
                                    }
                                }
                            ]
                        }
                    ],
                    'requested_speaker_capabilities': []
                }
            }
        ]
        ,
        'notifications': {
            'push': {}
        }
    }
    tst = session.put('https://iot.quasar.yandex.ru/m/v3/user/scenarios/' + scenario_id, json=data, headers=headers, cookies=cookies)
    print(tst.text)
    tst = session.post('https://iot.quasar.yandex.ru/m/user/scenarios/' + scenario_id + '/actions', headers=headers, cookies=cookies)
    print(tst.text)
    #Проверить изменение coockies в лог
    #print(response.cookies)
    #print(response.headers['Set-Cookie'])