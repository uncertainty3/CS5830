from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import re
import random
from zipfile import ZipFile
import shutil
# Define default_args and DAG parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2000, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id = 'climatological_data_fetch_page',
    default_args=default_args,
    description='Fetch location-wise datasets page for a specific year',
    schedule_interval=None,
) as dag:
    Year = '2000'
    p = f'/tmp/datapath.html'
    url = f'https://www.ncei.noaa.gov/data/local-climatological-data/access/2000'
    direc = '/tmp/data'

    fetch_page_task = BashOperator(
    task_id='fetch_page',
    bash_command= f'curl -s -L -o {p} {url}', 
    dag=dag
    )

    def select_random_data_files(ti):
        base_path = p
        with open(base_path, 'r') as file:
            html_content = file.read()
        
        csv_filenames = re.findall(r'<a href="([^"]+\.csv)">', html_content)

        ti.xcom_push(key='selected_files', value=csv_filenames)

    csv_files = PythonOperator(
        task_id='extract_csv_files',
        python_callable=select_random_data_files,
        provide_context=True,
    )

    def select_files2(n, ti):
        files = ti.xcom_pull(task_ids = 'extract_csv_files', key = 'selected_files')
        if n > len(files):
            raise ValueError(f"Cannot select: Too many requested files")
        ti.xcom_push(key = 'selected_files', value = random.sample(files, n))

    file_selector = PythonOperator(
        task_id='select_files',
        python_callable=select_files2,
        op_args=[10],
        provide_context=True,
    )

    def fetch_data_files(url, direc,ti):

        selected_files = ti.xcom_pull(task_ids='select_files', key='selected_files')
        for filename in selected_files:
            download_path = os.path.join(direc, filename)
            os.makedirs(direc, exist_ok=True)  # Create directory if it doesn't exist
            os.system(f"curl -s -L -o {download_path} {url}{filename}")
            print(download_path)

    fetch_data_tasks = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_files,
        op_args=[url, direc],
        provide_context=True,
    )
    def zip_files(ti):
        selected_files = ti.xcom_pull(task_ids='select_files', key='selected_files')
        zip_path = '/tmp/selected_files.zip'

        with ZipFile(zip_path, 'w') as zipf:
            for file in selected_files:
                file_path = os.path.join(direc, file)
                zipf.write(file_path, os.path.basename(file_path))

        ti.xcom_push(key='zip_path', value=zip_path)

    zip_files_task = PythonOperator(
        task_id='zip_files',
        python_callable=zip_files,
        provide_context=True,
    )

    def move_archive(destination_path, **kwargs):
        ti = kwargs.get('ti')
        try:
            # Retrieving the zip_path from XCom
            zip_path = ti.xcom_pull(task_ids="zip_files", key="zip_path")
            
            # Log statements for debugging
            print(f"Moving archive from {zip_path} to {destination_path}")

            # Moving the archive
            shutil.move(zip_path, destination_path)

            print("Move successful!")
        except Exception as e:
            print(f"Error moving archive: {e}")

    move_archive_task = PythonOperator(
        task_id='move_archive',
        python_callable=move_archive,
        op_args=['/tmp/finalloc.zip'],  # Destination path
        provide_context=True,
        dag=dag,  # Make sure to add the DAG instance
    )


    fetch_page_task >> csv_files >> file_selector >> fetch_data_tasks >> zip_files_task >> move_archive_task