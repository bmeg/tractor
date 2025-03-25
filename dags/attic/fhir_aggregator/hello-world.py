import os
import shutil
import tempfile

from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

outlets = [Dataset("hello-world-dataset")]

with DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # use our own temp dir
    tempdir = tempfile.mkdtemp()

    def before_command(context):
        """Change to the temp dir before executing the command."""
        os.chdir(tempdir)
        # TODO - download files to the temp dir
        # cwd = os.getcwd()
        # print(f"Before command execution\ncwd: {cwd}\ncontext: {context}\nti: {context['ti']}")


    def list_files_recursive(directory):
        file_paths = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                file_paths.append(os.path.join(root, file))
        return file_paths


    def on_failure_callback(context):
        """Alert the team on failure, clean up the temp dir."""
        cwd = os.getcwd()
        print(f"After Failure command execution\ncwd: {cwd}\ncontext: {context}\nti: {context['ti']}")


    def on_success_callback(context):
        """Update the outlets with the manifest, clean up the temp dir."""
        cwd = os.getcwd()
        print(f"After command execution\ncwd: {cwd}\ncontext: {context}\nti: {context['ti']}")
        state = context['ti'].state
        # update the outlets with the manifest
        _outlets = context['task'].outlets
        for outlet in _outlets:
            extra = {"manifest": list_files_recursive(tempdir)}
            context["outlet_events"][outlet].extra = extra
            print(f"Updated outlet {outlet} with extra: {extra}")

        # clean up the temp dir
        shutil.rmtree(tempdir)
        print(f"Removed {tempdir}")


    hello_task = BashOperator(
        task_id='hello',
        cwd=tempdir,
        bash_command='pwd; fa_submit prep --help; mkdir foo; echo "Hello World" > foo/hello.txt; exit 0',
        pre_execute=before_command,
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
        outlets=outlets,
        retries=0
    )

    hello_task
