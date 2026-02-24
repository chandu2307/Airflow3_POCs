from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.hitl import HITLBranchOperator
from airflow.providers.standard.operators.python import PythonOperator ,BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.sdk import Param  , Label
import runner.runner_approval_processing as runner


email_list = Variable.get("business_emails",deserialize_json=True)

with DAG(
        dag_id="payments_processing_dag",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        tags=["Payments","Business"],
        params={
            "processing_date": Param(
                datetime.now().strftime("%Y-%m-%d"),
                description= "provide processing date date for payments processing",
                title = "processing date",
                type = "string",
                format = "date"
            ),
            "feedback" : Param(
                default=" ",
                description="This Field is used to provided feedback by business for Approval Operator",
                title = "Feedback",
                type = "string"
            )
        }
):
    fetch_eligible_payments = BranchPythonOperator(
        task_id = "fetch_eligible_payments",
        python_callable = runner.fetch_required_data_and_generate_csv,
        op_kwargs = {
            "processing_date" : "{{ params.processing_date }}",
            "next_success" : "awaiting_business_approval",
            "failure_task" : "no_records_found"
        }
    )

    awaiting_business_approval = HITLBranchOperator(
        task_id="awaiting_business_approval",
        subject="Payments Review Required",
        body = "check your email for the approval link",
        options=["Approve","Reject"],
        options_mapping={
            "Approve" : "approved_business_ack",
            "Reject" : "pull_user_provided_feedback"
        },
        multiple=False,
        notifiers=[
            SmtpNotifier(
                to=email_list,
                from_email=email_list[0],
                subject="🚨 Approval Required: {{ ti.task_id }} for {{ params.processing_date }}",
                html_content="""
                <!DOCTYPE html>
                        <html><body>
                        <h2>Business Approval Needed</h2>
                        <p>Hi Team,</p>
                        <p>Payments data ready for <strong>{{ params.processing_date }}</strong></p>
                        <p>Please review and take action:</p>
                        <a href="http://localhost:8080/custom/hitl-ui?task_id=awaiting_business_approval&dag_id={{ ti.dag_id }}&dag_run_id={{ dag_run.run_id }}&processing_date={{ params.processing_date }}" 
                            style="background: linear-gradient(135deg, #10b981, #059669); color: white; padding: 20px 40px; border-radius: 16px; font-weight: 600; text-decoration: none; display: inline-block; box-shadow: 0 12px 30px rgba(16, 185, 129, 0.4); font-size: 16px; border: none; cursor: pointer;">
                            CLICK TO APPROVE/REJECT
                        </a>
                        <br><br>
                        <p><small><strong>Task:</strong> awaiting_business_approval<br>
                        <strong>DAG:</strong> {{ ti.dag_id }}<br>
                        <strong>Run:</strong> {{ dag_run.run_id }}</small></p>
                        <p>Thanks,<br>Automation Team</p>
                        <small>All rights reserved © 2026 Automation Team</small>
                </body></html>""",
                smtp_conn_id="smtp_conn",
                files=["{{ ti.xcom_pull(task_ids='fetch_eligible_payments', key='payments_file_path') }}"],
            )
        ],
    )

    pull_user_provided_feedback = PythonOperator(
        task_id="pull_user_provided_feedback",
        python_callable=runner.fetch_user_provided_feedback,
        op_kwargs={"next_success" : "rejected_business_ack"}
    )
    # alerts
    approved_business_ack = EmailOperator(
        task_id="approved_business_ack",
        to=email_list,
        from_email=email_list[0],
        subject="Approved Business Acknowledgement Receipt for {{ params.processing_date }}",
        html_content="""
                <!DOCTYPE html>
                        <html><body>
                        <p>Hi Team,</p>
                        <p>The Payments are approved by Business for <strong>{{ params.processing_date }}</strong></p>
                        <br><br>
                        <p><small><strong>Task:</strong> awaiting_business_approval<br>
                        <strong>DAG:</strong> {{ ti.dag_id }}<br>
                        <strong>Run:</strong> {{ dag_run.run_id }}</small></p>
                        <p>Thanks,<br>Automation Team</p>
                        <small>All rights reserved © 2026 Automation Team</small>
                </body></html>""",
        files= ["{{ ti.xcom_pull(task_ids='fetch_eligible_payments', key='payments_file_path') }}"],
        conn_id="smtp_conn"
    )

    rejected_business_ack = EmailOperator(
        task_id="rejected_business_ack",
        to=email_list,
        from_email=email_list[0],
        subject="Rejected Business Acknowledgement Receipt for {{ params.processing_date }}",
        html_content="""
                    <!DOCTYPE html>
                            <html><body>
                            <p>Hi Team,</p>
                            <p>The Payments are Rejected by Business for <strong>{{ params.processing_date }}</strong></p>
                            <br><br>
                            <p> <strong>Feedback given</strong> </p>
                            <p style="color: red;">{{ ti.xcom_pull(task_ids='pull_user_provided_feedback', key='feedback') }}</p>
                            <br></br>
                            <p><small><strong>Task:</strong> awaiting_business_approval<br>
                            <strong>DAG:</strong> {{ ti.dag_id }}<br>
                            <strong>Run:</strong> {{ dag_run.run_id }}</small></p>
                            <br></br>
                            <p>Thanks,<br>Automation Team</p>
                            <small>All rights reserved © 2026 Automation Team</small>
                    </body></html>""",
        files=["{{ ti.xcom_pull(task_ids='fetch_eligible_payments', key='payments_file_path') }}"],
        conn_id="smtp_conn"
    )

    no_records_found = EmptyOperator(task_id="no_records_found")

    #------------ WorkFlow-------------------------------------------------------------#
    fetch_eligible_payments >> awaiting_business_approval

    fetch_eligible_payments >> Label("No Eligible Payments Found") >>no_records_found

    awaiting_business_approval >> [approved_business_ack , pull_user_provided_feedback]

    pull_user_provided_feedback >> Label("User Provided Feedback for Rejection") >>rejected_business_ack
