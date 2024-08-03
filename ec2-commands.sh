sudo apt update
sudo apt install python3-pip
sudo apt install python3.12-venv
python3 -m venv venv
source venv/bin/activate
sudo chmod -R a+rwx airflow_snow_venv
sudo venv/bin/pip install apache-airflow
sudo venv/bin/pip install apache-airflow-providers-snowflake snowflake-connector-python apache-snowflake-sqlalchemy
airflow standalone