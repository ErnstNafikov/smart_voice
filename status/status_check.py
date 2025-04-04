import sys
import os
import requests
import subprocess
# Добавляем родительскую папку в sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from yandex import smart_voice
from ai import deepseek
from datetime import datetime

result = subprocess.run(['sh', '/root/process/status/docker_stat.sh'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
output, errors = result.stdout, result.stderr
docker_ps = output.decode("utf-8").splitlines()
#print(docker_ps,len(errors))
postgres_m = [x for x in docker_ps if "postgres" in x]
grafana_m = [x for x in docker_ps if "grafana" in x]
#print(len(postgres_m),len(grafana_m))
status_check = ''
if len(errors) > 0:
    status_check += errors.decode("utf-8")
else:
    if len(postgres_m) == 0:
        status_check += ' postgres - отключен,'
    # if len(grafana_m) == 0:
        # status_check += ' grafana - отключен,'

if len(status_check) > 0:
    status_check = 'Время ' + datetime.now().strftime("%H:%M") + status_check
    smart_voice._text_voice('e2cec051-72c8-430e-921a-2ec59e90e59a','Статус системы',status_check[:100])