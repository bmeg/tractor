# airflow config


## export
```bash
dc exec -it  airflow-worker airflow variables export config/exported_variables.json
dc exec -it  airflow-worker airflow connections export config/exported_connections.json
```

> Note:🙀 make sure you do not check in keys!
```
$ grep CHANGE config/*.json
config/exported_connections.json:    "login": CHANGE_ME-aws_access_key_id,
config/exported_connections.json:    "password": CHANGE_ME-aws_secret_access_key,
```


## import

```bash
dc exec -it  airflow-worker airflow variables import config/exported_variables.json
dc exec -it  airflow-worker airflow connections import config/exported_connections.json
```