"""
source code for HITL , plugins runner file
"""

import os
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


payments_dir = "/opt/airflow/tmp"
os.makedirs(payments_dir, exist_ok=True)
payments_file = f"{payments_dir}/airflow_payments_tmp.csv"

def fetch_required_data_and_generate_csv(
        processing_date : str,
        next_success : str,
        failure_task : str,
        ti)->str:
    """
    fetch required data from database and do the processing
    :param processing_date: calendar pick
    :param next_success: next success task
    :param failure_task: failure task
    :param ti: task instance
    :return: task_to_run
    """
    task_to_run = None
    try:
        logger.info("Fetch required data -- START -- ")

        sql = """
            SELECT * FROM public.payments
            where 
                amount > 2000 and 
                status = 'pending' and
                created_at::date = %s
            ORDER BY id ASC
        """

        hook = PostgresHook(postgres_conn_id='postgres_default')
        df = hook.get_pandas_df(sql,parameters=(processing_date,))
        if len(df)==0:
            logger.warning("⚠️ No records found for processing")
            task_to_run = failure_task
        else:
            logger.info("Records found for processing :: %s",{
                "processing_date": processing_date,
                "rows_fetched": len(df)
            })

            logger.info("💾 Generating CSV...")
            df.to_csv(payments_file,index=False, header=True)

            if os.path.exists(payments_file):
                logger.info("CSV file is generated .. pushing filepath to xcom")
                ti.xcom_push(key="payments_file_path",value=payments_file)
                task_to_run = next_success
            else:
                logger.warning("Failed to generate csv file",payments_file)
    except Exception as e:
        logger.error("❌ failed due to %s", e)
    finally:
        logger.info("Fetch required data -- END -- ")
        return task_to_run


def fetch_user_provided_feedback(next_success : str,ti)->str:
    task_to_run = None
    try:
        sql = """
                    SELECT params_input -> 'feedback' as feedback FROM public.hitl_detail
                    ORDER BY responded_at DESC LIMIT 1
                """

        hook = PostgresHook(postgres_conn_id='postgres_airflow')
        response = hook.get_first(sql)[0]
        logger.info(response)
        ti.xcom_push(key="feedback",value=response)
        task_to_run = next_success

    except Exception as e:
        logger.error("❌ failed due to %s", e)
    finally:
        logger.info("Fetch required data -- END -- ")
        return task_to_run