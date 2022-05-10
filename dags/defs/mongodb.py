def get_mongo_creds():
    with open("/opt/airflow/secrets/mongo_creds.txt", 'r') as f:
        mongo_pass = f.read()
        return mongo_pass