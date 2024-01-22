## Описание ошибок 

Возникли следующие ошибки:

1 Версия docker

`docker compose -f docker-compose.airflow.yaml up airflow-init`

`[+] Building 0.0s (0/0)`                                                         
`http: invalid Host header`

Исправила командой (у меня докер установлен через snap): 

`sudo snap refresh docker --channel=latest/edge`


2 
В процессоре GetFile:

*""... does not have sufficient permissions (i.e., not writable and readable): java.lang.IllegalStateException:..."*

Исправила, заменив процессор GetFile на комбинацию процессоров ListFile + FetchFile 
