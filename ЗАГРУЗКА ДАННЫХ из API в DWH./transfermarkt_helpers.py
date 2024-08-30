import requests
import boto3
import logging
import time
import datetime
from datetime import date
import dateutil
import json
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from string import Template
from io import BytesIO
from airflow.models import Connection
from airflow.exceptions import AirflowBadRequest


# Load data from API and save raw data to S3
API_CLUBS = "https://transfermarkt-api.vercel.app/competitions/gb1/clubs?season_id=2023"
API_CLUB_PROFILES = Template(
    "https://transfermarkt-api.vercel.app/clubs/$club_id/profile"
)
API_CLUB_PLAYERS = Template(
    "https://transfermarkt-api.vercel.app/clubs/$club_id/players?season_id=2023"
)
API_PLAYER_PROFILES = Template(
    "https://transfermarkt-api.vercel.app/players/$player_id/profile"
)
API_PLAYER_MARKET_VALUES = Template(
    "https://transfermarkt-api.vercel.app/players/$player_id/market_value"
)

s3 = Connection.get_connection_from_secrets("Minio")
S3_WORK_BUCKET = "transfermarkt"
S3_WORK_BUCKET = s3.schema
#S3_ENDPOINT = s3.host
S3_ACCESS_KEY = "minio"
S3_SECRET_KEY = "minio123"
S3_ENDPOINT = "http://minio:9000"
# S3_ACCESS_KEY = s3.login
# S3_SECRET_KEY = s3.password
ENCODING = "UTF-8"

#DT = "20240206"
DT = date.today().strftime("%Y%m%d")

CLUBS_METACOLS = ["id", "name", "seasonID"]
CLUB_PROFILES_COLS = [
    "id",
    "name",
    "officialName",
    "addressLine1",
    "addressLine2",
    "addressLine3",
    "stadiumName",
    "stadiumSeats",
    "currentMarketValue",
]
CLUB_PLAYERS_METACOLS = [
    "id",
]
CLUB_PLAYERS_COLS = ["id", "player_id"]
PLAYER_PROFILES_COLS = [
    "id",
    "name",
    "fullName",
    "dateOfBirth",
    "height",
    "isRetired",
    "foot",
    "placeOfBirth_country",
    "position_main",
    "club_joined",
    "club_contractExpires",
    "agent_name",
    "description",
    "nameInHomeCountry",
]

PLAYER_MARKET_VALUES_COLS = ["market_date", "market_value", "player_id"]


def get_raw_data_from_tm_and_save_to_s3(api: str, dt: str, filename: str) -> None:
    """Reads data from API and saves it to S3"""

    data = requests.get(api)
    if data.status_code == 200:
        s3 = boto3.resource(service_name = "s3", endpoint_url = S3_ENDPOINT, aws_access_key_id = S3_ACCESS_KEY, aws_secret_access_key = S3_SECRET_KEY)
        
        json_path = f"raw-data/{DT}/{filename}_raw.json"

        response = s3.Object(bucket_name = S3_WORK_BUCKET, key = json_path).put(Body = json.dumps(data.json()))
        
        time.sleep(2)

        print(f"API processed: {api}")
    else:
        logging.error(f"Couldn't load data from API {api}")
        #raise AirflowBadRequest()


def get_data_from_s3(key: str) -> dict:
    """Read data from S3"""

    # print(key)
    logging.info(f"Start loading {key}")
    session = boto3.session.Session()

    s3 = session.client(service_name = "s3", endpoint_url = S3_ENDPOINT, aws_access_key_id = S3_ACCESS_KEY, aws_secret_access_key = S3_SECRET_KEY)

    df = json.loads(s3.get_object(Bucket = S3_WORK_BUCKET, Key = key)["Body"].read().decode(ENCODING))

    logging.info(f"File {key} loaded")

    return df


def save_data_to_parquet_s3(dataf: pd.DataFrame, dt: str, filename: str) -> None:
    table = pa.Table.from_pandas(df=dataf)
    buf = BytesIO()

    pq.write_table(table, buf)

    s3 = boto3.resource(service_name = "s3", endpoint_url = S3_ENDPOINT, aws_access_key_id = S3_ACCESS_KEY, aws_secret_access_key = S3_SECRET_KEY)

    obj = s3.Object(bucket_name = S3_WORK_BUCKET, key = f"processed/{DT}/{filename}.parquet")

    obj.put(Body=buf.getvalue())


def filter_objects_by_prefix_in_s3(key_prefix: str) -> set:
    s3 = boto3.resource(
        service_name="s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )
    bucket = s3.Bucket(S3_WORK_BUCKET)

    objects = [obj for obj in bucket.objects.filter(Prefix=key_prefix)]

    return set(objects)
