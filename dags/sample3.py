from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='complex_parallel_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    init = BashOperator(task_id='init', bash_command='echo "Инициализация"')

    # Группа 1: Одиночная быстрая задача
    quick_task = BashOperator(task_id='quick_check', bash_command='echo "Ок"')

    # Группа 2: Цепочка задач (выполняется параллельно с остальными)
    step_1 = BashOperator(task_id='data_step_1', bash_command='echo "Загрузка"')
    step_2 = BashOperator(task_id='data_step_2', bash_command='echo "Очистка"')
    
    # Группа 3: Несколько параллельных вычислений
    calc_1 = BashOperator(task_id='calc_1', bash_command='echo "Расчет 1"')
    calc_2 = BashOperator(task_id='calc_2', bash_command='echo "Расчет 2"')
    calc_3 = BashOperator(task_id='calc_3', bash_command='echo "Расчет 3"')

    final_report = BashOperator(task_id='final_report', bash_command='echo "Отчет готов"')

    # Определение сложной структуры:
    
    # 1. От входа идем в три разные ветки
    init >> quick_task
    init >> step_1 >> step_2  # Линейная цепочка внутри ветки
    init >> [calc_1, calc_2, calc_3] # Веер из трех задач

    # 2. Собираем все ветки в финальную задачу
    [quick_task, step_2, calc_1, calc_2, calc_3] >> final_report
