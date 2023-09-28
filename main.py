'''Script connecting API and database'''
from flask import Flask, jsonify
from psycopg2 import connect, Error
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor


main = Flask(__name__)


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
