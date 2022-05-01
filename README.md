# Dokumentacja

## Pobierz repo

```sh
git clone https://github.com/wolpatryk/airflow-docker.git
cd airflow-docker
docker-compose up airflow-init
```

## Konfiguracja dockera

Uruchomienie obrazu **airflow-init**

```sh
docker-compose up airflow-init
```

Jeśli chcesz dograć nowe pakiety Pythona musisz dopisać pakiet do **requirements.txt** i przebudować obraz za pomocą polecenia:

```sh
docker-compose up --build airflow-init
```

Natomiast jeśli pakiety nadal nie będą zainstalowane, należy usunąć stary obraz **airflow-init** i zbudować obraz od nowa.

## w toku...

localhost:8080
airflow
airflow

localhost:5555

docker-compose -f C:\Users\patryk\um\airflow\docker-compose.yaml up --force-recreate --always-recreate-deps --build airflow-init