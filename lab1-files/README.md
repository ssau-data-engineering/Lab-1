## Описание ошибок 

Возникла следующая ошибка:
docker compose -f docker-compose.airflow.yaml up airflow-init
[+] Building 0.0s (0/0)                                                         
http: invalid Host header
Исправила командой (у меня докер установлен через snap): 
sudo snap refresh docker --channel=latest/edge 
