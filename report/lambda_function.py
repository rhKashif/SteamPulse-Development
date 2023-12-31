"""Python Script: Build a report for email attachment"""
from datetime import datetime, timedelta
from os import environ, _Environ

from functools import reduce
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import altair as alt
from altair.vegalite.v5.api import Chart
import boto3
import botocore.exceptions
from dotenv import load_dotenv
import pandas as pd
from pandas import DataFrame
from psycopg2 import connect
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor
from xhtml2pdf import pisa


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
            database=config_file["DATABASE_NAME"],
            user=config_file["DATABASE_USERNAME"],
            password=config_file["DATABASE_PASSWORD"],
            port=config_file["DATABASE_PORT"],
            host=config_file["DATABASE_ENDPOINT"]
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
    query = "SELECT\
            game.game_id, title, release_date, price, sale_price,\
            review_id, sentiment, review_text, reviewed_at, review_score,\
            genre, user_generated,\
            developer_name,\
            publisher_name,\
            mac, windows, linux\
            FROM game\
            LEFT JOIN review ON\
            review.game_id=game.game_id\
            LEFT JOIN platform ON\
            game.platform_id=platform.platform_id\
            LEFT JOIN game_developer_link as developer_link ON\
            game.game_id=developer_link.game_id\
            LEFT JOIN developer ON\
            developer_link.developer_id=developer.developer_id\
            LEFT JOIN game_genre_link as genre_link ON\
            game.game_id=genre_link.game_id\
            LEFT JOIN genre ON\
            genre_link.genre_id=genre.genre_id\
            LEFT JOIN game_publisher_link as publisher_link ON\
            game.game_id=publisher_link.game_id\
            LEFT JOIN publisher ON\
            publisher_link.publisher_id=publisher.publisher_id;"
    df_releases = pd.read_sql_query(query, conn_postgres)

    return df_releases


def format_database_columns(df_releases: DataFrame) -> DataFrame:
    """
    Format columns within the database to the correct data types

    Args:
        df_releases (DataFrame): A DataFrame containing filtered data related to new releases

    Returns:
        DataFrame: A DataFrame containing filtered data related to new releases 
        with columns in the correct data types
    """
    df_releases["release_date"] = pd.to_datetime(
        df_releases['release_date'], format='%d/%m/%Y')
    df_releases["review_date"] = pd.to_datetime(
        df_releases['reviewed_at'], format='%d/%m/%Y')

    return df_releases


def convert_html_to_pdf(source_html: str, output_filename: str) -> int:
    """
    Converts a html file to a PDF 

    Args:
        source_html (str): A file containing html formatted code

        output_filename (str): A string representing the desired output PDF file name

    Returns:
        int: An int value associate with an error code

    """
    result_file = open(output_filename, "w+b")

    pisa_status = pisa.CreatePDF(
        source_html,
        dest=result_file)

    result_file.close()

    return pisa_status.err


def get_data_for_release_date(df_releases: DataFrame, index: int) -> DataFrame:
    """
    Return a DataFrame for a specific date behind the current date

    Args:
        df_releases (DataFrame): A pandas DataFrame containing all relevant game data

        index (int): An integer representing the number of days to go back from current date

    Returns:
        DataFrame: A pandas DataFrame containing all relevant game data for a specific date
    """
    date = (datetime.now() - timedelta(days=index)).strftime("%Y-%m-%d")

    return df_releases[df_releases["release_date"] == date]


def get_data_for_release_date_range(df_releases: DataFrame, index: int) -> DataFrame:
    """
    Return a DataFrame for a range of dates behind the current date

    Args:
        df_releases (DataFrame): A pandas DataFrame containing all relevant game data

        index (int): An integer representing the number of days to go back from current date

    Returns:
        DataFrame: A pandas DataFrame containing all relevant game data for a specific date
    """
    date_week_ago = (datetime.now() - timedelta(days=index)
                     ).strftime("%Y-%m-%d")

    return df_releases[df_releases["release_date"] >= date_week_ago]


def get_number_of_new_releases(df_releases: DataFrame) -> int:
    """
    Return the number of new releases for the previous day

    Args: 
        df_releases (DataFrame): A pandas DataFrame containing all relevant game data

    Returns:
        int: An integer relating to the number of new games released
    """
    df_releases = get_data_for_release_date(df_releases, 1)

    return df_releases.drop_duplicates("title").shape[0]


def get_top_rated_release(df_releases: DataFrame) -> str:
    """
    Return the name of new release with the highest overall sentiment score for the previous day

    Args: 
        df_releases (DataFrame): A pandas DataFrame containing all relevant game data

    Returns:
        str: A string relating to the title of the highest rated new game released
    """
    df_releases = get_data_for_release_date_range(df_releases, 8)
    df_ratings = df_releases.groupby("title")["sentiment"].mean(
    ).sort_values(ascending=False).reset_index()

    return df_ratings.head(1)["title"][0]


def get_most_reviewed_release(df_releases: DataFrame) -> str:
    """
    Return the name of new release with the highest overall sentiment score for the previous day

    Args: 
        df_releases (DataFrame): A pandas DataFrame containing all relevant game data

    Returns:
        str: A string relating to the title of the highest rated new game released
    """
    df_releases = get_data_for_release_date_range(df_releases, 8)

    df_ratings = df_releases.groupby("title")["review_text"].count(
    ).sort_values(ascending=False).reset_index()

    return df_ratings.head(1)["title"][0]


def calculate_sum_sentiment(sentiment: float, score: int) -> float:
    """
    Calculates total sentiment score by multiplying sentiment associated with
    a review multiplied by the review_score (represents the number of users who
    agree with this review)

    Args:
        sentiment (float): A value associated with how positive or negative the
        review is considered to be

        score (int): A value associated with the number of users who up-voted a 
        review

    Returns:
        float: A sentiment value which takes into account the number of users
        who agreed with a given review
    """
    if score != 0:
        return sentiment * (score + 1)
    return sentiment


def aggregate_release_data_new_releases(df_releases: DataFrame) -> DataFrame:
    """
    Transform data in releases DataFrame to find aggregated data from individual releases.
    Does not contain the average sentiment or number of reviews. 

    Args:
        df_release (DataFrame): A DataFrame containing new release data

    Returns:
        DataFrame: A DataFrame containing new release data with aggregated data for each release
    """

    data_frames = [df_releases]

    df_merged = reduce(lambda left, right: pd.merge(left, right, on=['title'],
                                                    how='outer'), data_frames)

    df_merged = df_merged.drop_duplicates("title")

    desired_columns = ["title", "release_date",
                       "sale_price"]
    df_merged = df_merged[desired_columns]

    table_columns = ["Title", "Release Date",
                     "Price"]
    df_merged.columns = table_columns

    return df_merged


def aggregate_data(df_releases: DataFrame) -> DataFrame:
    """
    Transform data in releases DataFrame to find aggregated sentiment from individual reviews
    Args:
        df_release (DataFrame): A DataFrame containing new release data
    Returns:
        DataFrame: A DataFrame containing new release data with aggregated data for each release
    """

    df_releases["weighted_sentiment"] = df_releases.apply(lambda row:
                                                          calculate_sum_sentiment(row["sentiment"], row["review_score"]), axis=1)

    review_rows_count = df_releases.groupby(
        "game_id")["weighted_sentiment"].count()
    total_sum_scores = df_releases.groupby(
        "game_id")["weighted_sentiment"].sum()
    total_weights = df_releases.groupby("game_id")[
        "review_score"].sum()

    total_weights = total_weights + review_rows_count
    total_sentiment_scores = total_sum_scores / total_weights

    df_releases["avg_sentiment"] = df_releases["game_id"].apply(
        lambda row: round(total_sentiment_scores.loc[row], 1))

    review_per_title = df_releases.groupby('game_id')[
        'review_id'].nunique()
    review_per_title = review_per_title.to_frame()

    review_per_title.columns = ["num_of_reviews"]

    data_frames = [df_releases, review_per_title]

    df_merged = reduce(lambda left, right: pd.merge(left, right, on=['game_id'],
                                                    how='outer'), data_frames)

    df_merged.drop(["weighted_sentiment"], axis=1, inplace=True)

    df_merged = df_merged.drop_duplicates("title")

    desired_columns = ["title", "release_date",
                       "sale_price", "avg_sentiment", "num_of_reviews"]
    df_merged = df_merged[desired_columns]

    table_columns = ["Title", "Release Date",
                     "Price", "Community Sentiment", "Number of Reviews"]
    df_merged.columns = table_columns

    return df_merged


def format_columns(df_releases: DataFrame) -> DataFrame:
    """
    Format columns in DataFrame for display

    Args:
        df_release (DataFrame): A DataFrame containing new release data

    Returns:
        DataFrame: A DataFrame containing data with formatted columns
    """

    df_releases['Price'] = df_releases['Price'].apply(
        lambda x: f"£{x:.2f}")
    df_releases['Release Date'] = df_releases['Release Date'].dt.strftime(
        '%d/%m/%Y')
    if 'Community Sentiment' in df_releases.columns:
        df_releases['Community Sentiment'] = df_releases['Community Sentiment'].apply(
            lambda x: round(x, 2))
        df_releases['Community Sentiment'] = df_releases['Community Sentiment'].fillna(
            "No Sentiment")

    return df_releases


def plot_table(df_releases: DataFrame, rows: int) -> Chart:
    """
    Create a table from a given DataFrame

    Args:
        df_releases (DataFrame): A DataFrame containing filtered data for a chart

        rows (int): An integer value representing the number of rows to be displayed

    Returns:
        Chart: A chart displaying plotted table
    """
    chart = alt.Chart(
        df_releases.reset_index().head(rows)
    ).mark_text(fontSize=23, limit=450).transform_fold(
        df_releases.columns.tolist()
    ).encode(
        alt.X(
            "key",
            type="nominal",
            axis=alt.Axis(
                orient="top",
                labelAngle=0,
                title=None,
                ticks=False,
                labelFontSize=26,
                labelLimit=400
            ),
            scale=alt.Scale(padding=10),
            sort=None,
        ),
        alt.Y("index", type="ordinal", axis=None),
        alt.Text("value", type="nominal"),
    ).properties(
        height=1000,
        width=1300
    )
    return chart


def plot_table_small(df_releases: DataFrame, rows: int) -> Chart:
    """
    Create a table for a DataFrame looking at the top 5 games.

    Args:
        df_releases (DataFrame): A DataFrame containing filtered data for a chart

        rows (int): An integer value representing the number of rows to be displayed

    Returns:
        Chart: A chart displaying plotted table
    """
    chart = alt.Chart(
        df_releases.reset_index().head(rows)
    ).mark_text(fontSize=21, limit=450).transform_fold(
        df_releases.columns.tolist()
    ).encode(
        alt.X(
            "key",
            type="nominal",
            axis=alt.Axis(
                orient="top",
                labelAngle=0,
                title=None,
                ticks=False,
                labelFontSize=22,
                labelLimit=400
            ),
            scale=alt.Scale(padding=10),
            sort=None,
        ),
        alt.Y("index", type="ordinal", axis=None),
        alt.Text("value", type="nominal"),
    ).properties(
        width=1300,
        height=200
    )
    return chart


def plot_trending_games_sentiment_table(df_releases: DataFrame) -> None:
    """
    Create a table for the top recommended games by sentiment

    Args:
        df_releases (DataFrame): A DataFrame containing filtered data related to new releases

    Returns:
        Chart: A chart displaying plotted table
    """
    df_releases = get_data_for_release_date_range(df_releases, 8)
    df_merged = aggregate_data(df_releases)

    df_releases = df_merged.sort_values(
        by=["Community Sentiment"], ascending=False)
    df_releases = format_columns(df_releases)

    df_releases = df_releases.reset_index(drop=True)

    df_releases = df_releases[df_releases["Number of Reviews"] > 10]

    chart = plot_table_small(df_releases, 5)
    return chart


def plot_trending_games_review_table(df_releases: DataFrame) -> None:
    """
    Create a table for the top recommended games by number of reviews 

    Args:
        df_releases (DataFrame): A DataFrame containing filtered data related to new releases

    Returns:
        Chart: A chart displaying plotted table
    """
    df_releases = get_data_for_release_date_range(df_releases, 8)
    df_merged = aggregate_data(df_releases)
    df_releases = df_merged.sort_values(
        by=["Number of Reviews"], ascending=False)
    df_releases = format_columns(df_releases)

    df_releases = df_releases.reset_index(drop=True)

    chart = plot_table_small(df_releases, 5)
    return chart


def plot_new_games_today_table(df_releases: DataFrame) -> None:
    """
    Create a table for the new releases today

    Args:
        df_releases (DataFrame): A DataFrame containing filtered data related to new releases

    Returns:
        Chart: A chart displaying plotted table
    """
    df_releases = get_data_for_release_date(df_releases, 1)
    num_new_releases = get_number_of_new_releases(df_releases)

    df_merged = aggregate_release_data_new_releases(df_releases)

    df_releases = df_merged.sort_values(
        by=["Release Date"], ascending=False)
    df_releases = format_columns(df_releases)

    df_releases = df_releases.reset_index(drop=True)
    df_releases = df_releases.head(27)

    chart = plot_table(df_releases, num_new_releases)
    return chart


def build_figure_from_plot(plot: Chart, figure_name: str) -> str:
    """
    Build a .png file for a plot and return its path

    Args:
        plot (Chart): A chart displaying plotted data

        figure_name (str): A string for use as the name of the created .png file

    Return:
        str: A string representing the path of a .png file
    """
    plot.save(f"/tmp/{figure_name}.png")
    return f"/tmp/{figure_name}.png"


def create_report(df_releases: DataFrame, dashboard_url: str) -> None:
    """
    Create a pdf file from a html string

    Args:  
        df_releases (DataFrame): A DataFrame containing new release information

        dashboard_url (str): A str representing a url link for the dashboard

    Returns:
        None
    """

    new_releases = get_number_of_new_releases(df_releases)
    top_rated_release = get_top_rated_release(df_releases)
    most_reviewed_release = get_most_reviewed_release(df_releases)

    new_release_table_plot = plot_new_games_today_table(df_releases)
    trending_release_sentiment_table_plot = plot_trending_games_sentiment_table(
        df_releases)
    trending_release_review_table_plot = plot_trending_games_review_table(
        df_releases)

    new_release_table_fig = build_figure_from_plot(
        new_release_table_plot, "table_one")

    trending_release_sentiment_fig = build_figure_from_plot(
        trending_release_sentiment_table_plot, "table_two")
    trending_release_review_fig = build_figure_from_plot(
        trending_release_review_table_plot, "table_three")

    date = (datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d")
    date_range = (datetime.now() - timedelta(days=8)).strftime("%Y/%m/%d")

    header_color = "#1b2838"
    text_color = "#f5f4f1"

    template = f'''
    <html>
    <head>
        <style>
            @page {{
                size: letter portrait;
                @frame header_frame {{           /* Static frame */
                    -pdf-frame-content: header_content;
                    left: 50pt; width: 512pt; top: 50; height: 160pt;
                }}
                @frame col1_frame {{             /* Content frame 1 */
                    left: 50pt; width: 512pt; top: 160pt; height: 505pt;
                }}
                @frame footer_frame {{           /* Static frame */
                    -pdf-frame-content: footer_content;
                    left: 50pt; width: 512pt; top: 656pt; height: 200pt;
                }}  
            }}          
            body {{
                font-family: Arial, sans-serif;
                font-size: 14px;
                text-align: left;
            }}
            h1 {{
                background-color: {header_color};
                color: {text_color};
                padding-bottom: 25px;
                padding-top: 50px;
                font-size: 32px;
                text-align: center;
                }}            
            h2 {{
                background-color: {header_color};
                color: {text_color};
                padding-top: 10px;
                font-size: 11px;
                text-align: center;
            }} 
            h3 {{
                background-color: {header_color};
                color: {text_color};
                padding-bottom: 5px;
                padding-top: 20px;
                font-size: 22px;
                text-align: center;
                }} 
            .myDiv {{
                border: 1px solid black; 
                padding: 5px;
                background-color: #f5f5f5;
            }}   
            .myDiv2 {{
                text-align: center;
            }}            
            </style>
    </head>

    <body>
        <div id="header_content">
            <h1>New Release Report</h1>
        </div>

        <div class = "myDiv2">
                    <p>Number of New Releases ({date}): {new_releases}<br>
                    Top Rated Release ({date_range} - {date}): {top_rated_release}<br>
                    Most Reviewed Release ({date_range} - {date}): {most_reviewed_release}</p>
                </div>

        <h2>Latest Releases</h2>
        <img src="{new_release_table_fig}" alt="Chart 1">

        <h2>Top Releases by Sentiment</h2>
        <img src="{trending_release_sentiment_fig}" alt="Chart 2">

        <h2>Top Releases by Number of Reviews</h2>
        <img src="{trending_release_review_fig}" alt="Chart 3">
        
        <div id="footer_content">
            <h3><a href={dashboard_url}>SteamPulse Dashboard</a></h3>
        SteamPulse - page <pdf:pagenumber>
        </div>


    </body>    
    </html>
    '''
    convert_html_to_pdf(template, environ.get("REPORT_FILE"))


def send_email(config: _Environ, email: str):
    """
    Send an email with an attached PDF report using Amazon Simple Email Service (SES).

    Args:
        config (_Environ): A file containing environment variables

    Returns:
        None
    """

    BODY_TEXT = "Good morning!\r\n\nPlease see the attached file for your latest report on newly released games.\n\nBest regards,\nSteamPulse Team"
    CHARSET = "utf-8"

    date = datetime.now().strftime("%d/%m/%Y")

    client = boto3.client("ses",
                          region_name="eu-west-2",
                          aws_access_key_id=config["ACCESS_KEY_ID"],
                          aws_secret_access_key=config["SECRET_ACCESS_KEY"])

    message = MIMEMultipart('mixed')
    message["Subject"] = f"SteamPulse: Latest Game Releases - {date}"

    message_body = MIMEMultipart('alternative')

    textpart = MIMEText(BODY_TEXT.encode(CHARSET), 'plain', CHARSET)
    message_body.attach(textpart)

    attachment = MIMEApplication(open(environ.get("REPORT_FILE"), 'rb').read())
    attachment.add_header('Content-Disposition',
                          'attachment', filename='SteamPulse_daily_report.pdf')

    message.attach(message_body)

    message.attach(attachment)

    client.send_raw_email(
        Source=config["EMAIL_SENDER"],
        Destinations=[
            email,
        ],
        RawMessage={
            'Data': message.as_string()
        }
    )


def get_list_of_emails_from_database(conn: connection) -> list[str]:
    """List returning a list of emails from the database"""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        email_list = []
        cur.execute("""SELECT email FROM user_email""")
        emails = cur.fetchall()
        cur.close()
        if emails:
            email_list = [item['email'] for item in emails]
        return email_list


def verify_email(config: _Environ, email: str):
    """Function to verify user email for subscription list"""
    client = boto3.client("ses",
                          region_name="eu-west-2",
                          aws_access_key_id=config["ACCESS_KEY_ID"],
                          aws_secret_access_key=config["SECRET_ACCESS_KEY"])

    response = client.verify_email_identity(
        EmailAddress=email
    )
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print('Verification Email Success.')
    else:
        print('Verification Error.')


def email_subscribers(conn: connection, config: _Environ):
    """Emails all subscribers either the report or verification email"""
    all_emails = get_list_of_emails_from_database(conn)
    verification_awaited = []
    for address in all_emails:
        try:
            send_email(config, address)
            print("Report email sent.")
        except botocore.exceptions.ClientError as err:
            if "MessageRejected" in str(err):
                verification_awaited.append(address)
            else:
                print(err)

    for address in verification_awaited:
        verify_email(config, address)


def handler(event, context) -> None:
    """
    AWS Lambda function to generate a report, send it via email using Amazon SES.

    Args:
        event: AWS Lambda event
        context: AWS Lambda context  

    Return:
        None
    """
    try:
        load_dotenv()
        config = environ

        conn = get_db_connection(config)
        game_df = get_database(conn)
        game_df = format_database_columns(game_df)

        create_report(game_df, config["DASHBOARD_URL"])
        print("Report created.")

        email_subscribers(conn, config)
    finally:
        conn.close()


if __name__ == "__main__":

    handler(None, None)
