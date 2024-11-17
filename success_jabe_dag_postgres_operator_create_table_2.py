from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor

default_argus= {
    'owner':'jabe',
    'retries': 20,
    'retry_delay': timedelta(minutes=5)
}

with DAG(dag_id='success_megaman_battlenetwork_create_table',
         default_args=default_argus,
         start_date=datetime(2024, 11, 10),
         end_date=datetime(2024, 11, 25, 23, 59),
         schedule_interval=None,
         catchup=False
         ) as dag:
    
    #precheck = FileSensor(
    #    task_id='check_for_datafile',
    #    filepath='/opt/airflow/data/standard_chip_db.csv',
    #    fs_conn_id='fs_default',
    #    mode='poke',
        
    #)

    migrate_battlenetwork_create_table = PostgresOperator(
        task_id='migrate_battlenetwork_create_table',
        postgres_conn_id='postgres_default',
        database='airflow',
        sql="""
            CREATE TABLE IF NOT EXISTS standard_chip_db_2_test(
                standard_chip_id VARCHAR NOT NULL,
                class VARCHAR NOT NULL,
                chip_name VARCHAR NOT NULL,
                damage VARCHAR,
                chip_code VARCHAR NOT NULL,
                memory VARCHAR NOT NULL)
        """
    )

    migrate_battlenetwork_data_csv = PostgresOperator(
        task_id='migrate_battlenetwork_data_csv',
        postgres_conn_id='postgres_default',
        database='airflow',
        sql="""
            COPY standard_chip_db_2_test
            FROM 'D:\JRJ\Data_Analysis_and_Data_Engineering\Data_Camp\My Database\standard_chip_db.csv'
            DELIMITER ','
            CSV HEADER;
        """
    )
    #precheck >> 
    migrate_battlenetwork_create_table >> migrate_battlenetwork_data_csv

    #note
# FROM 'D:/JRJ/Data_Analysis_and_Data_Engineering/Data_Camp/My Database/standard_chip_db.csv' ##forwards slash('/') will succeed
# FROM 'D:\JRJ\Data_Analysis_and_Data_Engineering\Data_Camp\My Database\standard_chip_db.csv' ##backward slash('\') will succeed
# '/opt/airflow/data/standard_chip_db.csv' # connection will refuse
# since your local postgres db have access to your local folder, you can always specificy the full path of the csv file