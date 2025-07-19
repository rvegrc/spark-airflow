helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow --create-namespace \  
  -f values.yaml