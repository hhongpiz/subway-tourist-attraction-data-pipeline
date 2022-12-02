from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
with DAG(
    'weather_predict_daily',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['hhongpizza@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2, #재시도횟수
        'retry_delay': timedelta(minutes=3), # 재시도 딜레이
    },
    description='Weather Predict ETL',
    schedule=timedelta(days=1),
    start_date=datetime(2022, 11, 9, 00, 30),
    catchup=False,
    tags=['weather_predict'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    start = BashOperator(
        task_id='start',
        bash_command='date',
    )

    ### extract ###

    t1 = BashOperator(
        task_id='extract_weather',
        cwd='/home/big/study/vscode',
        bash_command='python3 main.py extract extract_weather_now',
    )


    ### transform ###

    t2 = BashOperator(
        task_id='transform_weather',
        cwd='/home/big/study/vscode',
        bash_command='python3 main.py transform transform_weather_now',
    )

    t1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    **Image Credit:** Randall Munroe, [XKCD](https://xkcd.com/license.html)
    """
    )

    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG; OR
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )
    
    start >> t1 >> t2 