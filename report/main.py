"""Python Script: Build a report for email attachment"""
import base64
from datetime import datetime
from os import environ, _Environ

import altair as alt
from altair.vegalite.v5.api import Chart
from dotenv import load_dotenv
import pandas as pd
from pandas import DataFrame
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import streamlit as st
from psycopg2 import connect
from psycopg2.extensions import connection


from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

from xhtml2pdf import pisa
import plotly_express as px
import boto3


def get_db_connection(config_file: _Environ) -> connection:
    """
    Returns connection to the database

    Args:
        config (_Environ): A file containing sensitive values

    Returns:
        connection: A connection to a Postgres database
    """
    try:
        return connect(
            database=config_file["DB_NAME"],
            user=config_file["DB_USER"],
            password=config_file["DB_PASSWORD"],
            port=config_file["DB_PORT"],
            host=config_file["DB_HOST"]
        )
    except Exception as err:
        print("Error connecting to database.")
        raise err


def get_database(conn_postgres: connection) -> DataFrame:
    """
    Returns redshift database transaction table as a DataFrame Object

    Args:
        conn_postgres (connection): A connection to a Postgres database

    Returns:
        DataFrame:  A pandas DataFrame containing all relevant release data
    """
    query = f""
    df_releases = pd.read_sql_query(query, conn_postgres)

    return df_releases


def convert_html_to_pdf(source_html, output_filename):
    # open output file for writing (truncated binary)
    result_file = open(output_filename, "w+b")

    # convert HTML to PDF
    pisa_status = pisa.CreatePDF(
        source_html,                # the HTML to convert
        dest=result_file)           # file handle to recieve result

    # close output file
    result_file.close()                 # close output file

    return pisa_status.err


def create_report():
    img_bytes = px.scatter([1, 2, 3], [4, 5, 6]).to_image()
    img = base64.b64encode(img_bytes).decode("utf-8")
    template = f'''
    <h1>TMNT</h1>
    <p>Example!</p>
    <img style="width: 400; height: 600" src="data:image/png;base64,{img}">
    '''

    convert_html_to_pdf(template, environ.get("REPORT_FILE"))


def send_email():

    client = boto3.client("ses",
                          region_name="eu-west-2",
                          aws_access_key_id=environ["AWS_ACCESS_KEY_ID"],
                          aws_secret_access_key=environ["AWS_SECRET_ACCESS_KEY"])

    message = MIMEMultipart()
    message["Subject"] = "Local Test"

    attachment = MIMEApplication(open(environ.get("REPORT_FILE"), 'rb').read())
    attachment.add_header('Content-Disposition',
                          'attachment', filename='report.pdf')
    message.attach(attachment)

    print(message)

    client.send_raw_email(
        Source='trainee.hassan.kashif@sigmalabs.co.uk',
        Destinations=[
            'trainee.hassan.kashif@sigmalabs.co.uk',
        ],
        RawMessage={
            'Data': message.as_string()
        }
    )


def handler(event, context):
    create_report()
    print("Report created.")
    send_email()
    print("Email sent.")


if __name__ == "__main__":
    # Temporary mock data
    game_df = pd.read_csv("mock_data.csv")
    game_df["release_date"] = pd.to_datetime(
        game_df['release_date'], format='%d/%m/%Y')
    game_df["review_date"] = pd.to_datetime(
        game_df['review_date'], format='%d/%m/%Y')

    # Start of dashboard script
    load_dotenv()
    config = environ

    # conn = get_db_connection(config)

    # game_df = get_database(conn)

    print(handler(None, None))
