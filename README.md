# Payments Processing DAG -- Human-in-the-Loop (HITL) Approval

Apache Airflow 3.1.x + FastAPI Custom Plugin

------------------------------------------------------------------------
## 🎥 Demo

[![Watch the demo]](https://www.youtube.com/watch?v=iAmgca6YnaA)

## Setup 

Please follow official Apache Airflow documentation for Docker compose Setup for Local environment

## Overview

This project implements a Human-in-the-Loop (HITL) approval workflow
using Apache Airflow 3.1.x.

The DAG processes eligible payments and requires Business approval
before proceeding. It leverages:

-   HITLBranchOperator (Airflow 3.1+ feature)
-   Custom UI built using a FastAPI plugin
-   SMTP-based email notifications
-   XCom-based file sharing
-   Dynamic branching
-   Parameter-driven execution

------------------------------------------------------------------------

## Workflow Architecture

<img width="1750" height="722" alt="image" src="https://github.com/user-attachments/assets/82d7593a-a2f8-4071-aa8d-9d2a08b1db1e" />


------------------------------------------------------------------------

## DAG Configuration

DAG ID: payments_processing_dag\
Start Date: 2026-01-01\
Schedule: Manual Trigger Only\
Tags: Payments, Business

------------------------------------------------------------------------

## DAG Parameters

-   processing_date (string, date format): Date for payment processing
-   feedback (string): Captures rejection feedback from Plugin
  
<img width="1750" height="722" alt="image" src="https://github.com/user-attachments/assets/971f0dc7-49d9-4ff3-ad9f-e7285b66c0ba" />



------------------------------------------------------------------------

## Task Breakdown

### fetch_eligible_payments

-   BranchPythonOperator
-   Fetches payment data
-   Generates CSV file
-   Pushes file path via XCom
-   Branches to awaiting_business_approval or no_records_found

### awaiting_business_approval (HITLBranchOperator)

Options: - Approve → approved_business_ack - Reject →
pull_user_provided_feedback

Behavior: - Sends approval email - Attaches generated CSV - Waits for
business decision

### approved_business_ack

-   Sends approval acknowledgment email
-   Attaches payment file

### pull_user_provided_feedback

-   Retrieves feedback from custom UI
-   Passes control to rejected_business_ack

### rejected_business_ack

-   Sends rejection acknowledgment email
-   Includes feedback in email body
-   Attaches payment file

### no_records_found

Executed when no eligible payments exist.

------------------------------------------------------------------------

## Email Integration

Uses: - SmtpNotifier - EmailOperator

Features: - Dynamic subject templating - HTML formatted emails - File
attachments via XCom

------------------------------------------------------------------------

## Security

-   Custom FastAPI plugin restricts approval UI access
-   Role-based authorization enforced in plugin
-   SMTP credentials stored in Airflow Connections
-   Email recipients stored in Airflow Variables

------------------------------------------------------------------------

## Project Structure

dags/ payments_processing_dag.py

plugins/ custom_hitl_plugin/ fastapi_app.py approval_ui.py

runner/ runner_approval_processing.py

------------------------------------------------------------------------

## Requirements

-   Apache Airflow 3.1.x
-   Configured SMTP connection (smtp_conn)
-   Airflow Variable: business_emails
-   Custom FastAPI plugin enabled

------------------------------------------------------------------------

## How to Run

1.  Deploy DAG to Airflow 3.1 environment.
2.  Configure smtp_conn and business_emails variable.
3.  Trigger DAG manually.
4.  Business receives approval email.
5.  Business clicks custom approval link.
6.  DAG branches based on decision.

------------------------------------------------------------------------

## Summary

This project demonstrates:

-   Native Human-in-the-Loop implementation in Airflow 3.1
-   Clean branching logic with HITL
-   Email-driven approval loop
-   Custom FastAPI-based approval UI
-   Production-ready orchestration design
