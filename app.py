'''Script connecting API and database'''
from os import environ

from datetime import datetime, timedelta
from dotenv import load_dotenv
from flask import Flask, jsonify
import pandas as pd
from psycopg2 import connect, Error
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor


app = Flask(__name__)
load_dotenv()
config = environ


def get_db_connection(config) -> connection:
    """Connect to the database with game data"""
    try:
        return connect(
            user=config['DATABASE_USERNAME'],
            password=config['DATABASE_PASSWORD'],
            host=config['DATABASE_ENDPOINT'],
            port=config['DATABASE_PORT'],
            database=config['DATABASE_NAME'],
            cursor_factory=RealDictCursor)
    except (Error, ValueError) as err:
        return f"Error connecting to database. {err}"


@app.route("/", methods=["GET"])
def database():
    '''return database values'''
    conn = get_db_connection(config)
    with conn.cursor() as cur:
        cur.execute("""SELECT title FROM game;""")
        titles = cur.fetchall()
        return jsonify(titles)


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True, port=5050)
