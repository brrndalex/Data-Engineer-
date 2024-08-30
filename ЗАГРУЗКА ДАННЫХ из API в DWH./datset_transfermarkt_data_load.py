import pandas as pd
from airflow.models import DAG
from datetime import datetime
import logging
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task, task_group
from airflow.providers.postgres.operators.postgres import PostgresOperator
import custom.transfermarkt_helpers as th
from airflow.datasets import Dataset

DT = date.today().strftime("%Y%m%d")

CLUBS = Dataset("greenplum://stg_clubs")
CLUBS_PROFILES = Dataset("greenplum://stg_club_profiles")
CLUBS_PLAYERS = Dataset("greenplum://stg_club_players")
PLAYER_PROFILES = Dataset("greenplum://stg_player_profiles")
PLAYER_OTHER_POSITIONS = Dataset("greenplum://stg_player_other_positions")
PLAYER_CITIZENSHIP = Dataset("greenplum://stg_player_citizenship")
PLAYER_MARKET_VALUES = Dataset("greenplum://stg_market_values")




@task(task_id="loading_clubs")
def load_clubs(api, filename):
    """Loading clubs from API"""
    th.get_raw_data_from_tm_and_save_to_s3(api, th.DT, filename)


@task(task_id="loading_club_profiles")
def load_club_profiles(api, filename):
    """Loading clubs' profiles from API"""

    # get list of clubs' ids from clubs_raw.json
    club_ids = set([a["id"] for a in th.get_data_from_s3(key=f"raw-data/{th.DT}/clubs_raw.json")["clubs"]])

    # Get list of loaded clubs' profiles to not loading them again after failures
    club_profiles = set()
    for club_profile in th.filter_objects_by_prefix_in_s3(
        key_prefix=f"raw-data/{th.DT}/club-profiles"
    ):
        club_profiles.add(th.get_data_from_s3(club_profile.key)["id"])

    # Loading clubs' profiles
    for club_id in club_ids.difference(club_profiles):
        th.get_raw_data_from_tm_and_save_to_s3(
            api=api.substitute(club_id=club_id),
            dt=th.DT, # ds_nodash
            filename=f"{filename}_{club_id}",
        )

@task(task_id="loading_club_players")
def load_club_players(api, filename):

    club_ids = set([a["id"] for a in th.get_data_from_s3(key=f"raw-data/{th.DT}/clubs_raw.json")["clubs"]])

    logging.info(f"Всего клубов - {len(club_ids)}")

    club_players = set()
    for club_player in th.filter_objects_by_prefix_in_s3(key_prefix=f"raw-data/{th.DT}/club-players"):
        club_players.add(th.get_data_from_s3(club_player.key)["id"])

    for club_id in club_ids.difference(club_players):
        th.get_raw_data_from_tm_and_save_to_s3(api=api.substitute(club_id=club_id), dt=th.DT,
                                              filename=f"{filename}_{club_id}") 


# @task(task_id = "loading_club_players")
# def load_club_players():
#     """Loading clubs' players from API"""
    
#     clubs_ids = [i["id"] for i in th.get_data_from_s3(key=f"raw-data/{th.DT}/clubs_raw.json")["clubs"]]

#     print(clubs_ids)

#     for i in clubs_ids:
#         club_players = r.get(f"https://transfermarkt-api.vercel.app/clubs/{i}/players").json()
#         obj = s3.object(bucket_name ="transfermarkt", key =f"raw-data/{DT}/club_players/club_players_{i}.json")
#         obj.put(Body = json.dumps(club_players).encode())

@task(task_id="loading_player_profiles")
def load_player_profiles(api, filename) -> None:
    player_loaded_ids = set()
    for player_profile in th.filter_objects_by_prefix_in_s3(key_prefix=f"raw-data/{th.DT}/player-profiles"):
        player_loaded_ids.add(th.get_data_from_s3(player_profile.key)["id"])

    print(len(player_loaded_ids))

    player_ids = set()
    for club_player in th.filter_objects_by_prefix_in_s3(key_prefix=f"raw-data/{th.DT}/club-players"):
        players = th.get_data_from_s3(club_player.key).get("players")
        if players:
            player_ids.update([i["id"] for i in players])
        else:
            print(th.get_data_from_s3(club_player.key))
            break

    for player_id in player_ids.difference(player_loaded_ids):
        th.get_raw_data_from_tm_and_save_to_s3(api=api.substitute(player_id=player_id), dt=th.DT,
                                              filename=f"{filename}_{player_id}")
        

        

@task(task_id="loading_player_market_values")       
def load_player_market_values(api, filename) -> None:

    player_ids = set()
    for club_player in th.filter_objects_by_prefix_in_s3(key_prefix=f"raw-data/{th.DT}/club-players"):
        players = th.get_data_from_s3(club_player.key)["players"]
        player_ids.update([i["id"] for i in players])

    loaded_ids = set()
    for player_market_value in th.filter_objects_by_prefix_in_s3(key_prefix=f"raw-data/{th.DT}/player-market-values"):
        loaded_ids.add(th.get_data_from_s3(player_market_value.key)["id"])

    print(len(loaded_ids))

    for player_id in player_ids.difference(loaded_ids):
        th.get_raw_data_from_tm_and_save_to_s3(api=api.substitute(player_id=player_id), dt=th.DT,
                                              filename=f"{filename}_{player_id}")
        




@task(task_id="transform_clubs")
def transform_clubs(metacols) -> None:
    """Transforming clubs raw data to parquet"""

    clubs = th.get_data_from_s3(key=f"raw-data/{th.DT}/clubs_raw.json")
    df = pd.json_normalize(
        clubs,
        meta=metacols,
        meta_prefix="league_",
        record_path="clubs",
        record_prefix="club_",
    )
    th.save_data_to_parquet_s3(dataf=df, dt=th.DT, filename="clubs")


@task(task_id="transform_club_profiles")
def transform_club_profiles(cols) -> None:
    """Transforming clubs' profiles raw data to parquet"""
    club_profiles = []
    for i in th.filter_objects_by_prefix_in_s3(f"raw-data/{th.DT}/club-profiles"):
        club_profiles.append(th.get_data_from_s3(i.key))

    logging.info(club_profiles)
    df = pd.json_normalize(club_profiles, max_level=0)[cols]
    th.save_data_to_parquet_s3(dataf=df, dt=th.DT, filename="club_profiles")


# @task(task_id="transform_club_players")
# def transform_club_players(ds_nodash=None) -> None:
#     """Transforming clubs' players raw data to parquet"""
#     club_profiles = []
#     for i in th.filter_objects_by_prefix_in_s3(f"raw-data/{ds_nodash}/club-profiles"):
#         club_profiles.append(th.get_data_from_s3(i.key))

#     df = pd.json_normalize(club_profiles, meta = 'id', meta_prefix = 'club_', record_path= 'players', record_prefix = 'player_')[['club_id', 'player_id']]
#     th.save_data_to_parquet_s3(dataf=df, dt=th.DT, filename="club_players")
    

@task(task_id="transform_club_players")
def transform_club_players(prefix:str, filename:str, metacols:list, cols:list) -> None:
    """Transforming clubs' players raw data to parquet """
    club_players = []
    for i in th.filter_objects_by_prefix_in_s3(key_prefix=f"raw-data/{th.DT}/{prefix}"):
        club_players.append(th.get_data_from_s3(i.key))

    df = pd.json_normalize(club_players, meta=metacols, record_path="players", record_prefix="player_")[cols]
    th.save_data_to_parquet_s3(df, dt=th.DT, filename=filename)

    logging.info("Club players transformed")

@task(task_id="transform_player_profiles")
def transform_player_profiles(prefix: str, filename:str,  cols) -> None:
    player_profiles = []
    for i in th.filter_objects_by_prefix_in_s3(key_prefix=f"raw-data/{th.DT}/{prefix}"):
        a = th.get_data_from_s3(i.key)
        a.setdefault('dateOfBirth')
        player_profiles.append(a)

    player_profiles = pd.json_normalize(player_profiles, sep="_")[cols]
    player_profiles = player_profiles.rename(columns={'name': 'short_name'})
    player_profiles['dateOfBirth'] = player_profiles['dateOfBirth'].astype(str)
    th.save_data_to_parquet_s3(player_profiles, th.DT, filename)

    logging.info("players transformed")

@task(task_id="transform_player_other_positions")
def transform_player_other_positions(prefix:str, filename:str) -> None:
    player_profiles = []
    for i in th.filter_objects_by_prefix_in_s3(key_prefix=f"raw-data/{th.DT}/{prefix}"):
        content = th.get_data_from_s3(i.key)
        if content["position"].get('other'):
            player_profiles.append(th.get_data_from_s3(i.key))

    player_other_position = pd.json_normalize(player_profiles, record_path=["position", ["other"]], meta="id",
                                              meta_prefix="player_")
    
   
    player_other_position = player_other_position.rename(columns={0: "position"})
    th.save_data_to_parquet_s3(player_other_position, th.DT, filename)

    logging.info("positions transformed")

@task(task_id="transform_player_citizenship")
def transform_player_citizenship(prefix:str, filename:str) -> None:
    player_profiles = []
    for i in th.filter_objects_by_prefix_in_s3(key_prefix=f"raw-data/{th.DT}/{prefix}"):
        player_profiles.append(th.get_data_from_s3(i.key))

    player_citizenship = pd.json_normalize(player_profiles, meta="id", meta_prefix="player_", record_path="citizenship")
    player_citizenship = player_citizenship.rename(columns={0: "citizenship"})
    th.save_data_to_parquet_s3(player_citizenship, th.DT, filename)

    logging.info("citizenships transformed")

@task(task_id="transform_player_market_values")
def transform_player_market_values(prefix: str, filename: str) -> None:
    player_market_values_list = []
    for i in th.filter_objects_by_prefix_in_s3(key_prefix=f"raw-data/{th.DT}/{prefix}"):
        mv = th.get_data_from_s3(i.key)
        if mv.get("marketValueHistory"):
            player_market_values_list.append(mv)

    player_market_values = pd.json_normalize(player_market_values_list, meta="id", meta_prefix="player_",
                                             record_path="marketValueHistory", record_prefix="market_")

    player_market_values = player_market_values[th.PLAYER_MARKET_VALUES_COLS]
    print(player_market_values)
    th.save_data_to_parquet_s3(player_market_values, th.DT, filename=filename)

    logging.info("market values transformed")



with DAG(
    dag_id="datset_transfermarkt_data_load",
    start_date=datetime(2024, 1, 26),  # должна быть до реальной даты старта
    schedule="@weekly",
    catchup=False # чтобы начать с текущей даты, а не с start_date
) as dag:
    start_loading = EmptyOperator(task_id="Start_loading")
    api_loaded = EmptyOperator(task_id="API_loaded")    
    finish_loading = EmptyOperator(task_id="Finish_loading")

    load_clubs = load_clubs(api=th.API_CLUBS, filename="clubs")
    load_club_profiles = load_club_profiles(api=th.API_CLUB_PROFILES, filename="club-profiles/club_profile")
    load_club_players = load_club_players(api=th.API_CLUB_PLAYERS, filename="club-players/club_players")
    load_player_profiles = load_player_profiles(api=th.API_PLAYER_PROFILES, filename="player-profiles/player_profiles")
    load_player_market_values = load_player_market_values(api=th.API_PLAYER_MARKET_VALUES, filename="player-market-values/player_market_values")


    @task_group(group_id="transform_to_parquet")    
    def transform_to_parquet():
        """ Task Group for union transforming tasks"""
        
        clubs = transform_clubs(th.CLUBS_METACOLS)
        club_profiles = transform_club_profiles(th.CLUB_PROFILES_COLS)
        club_players = transform_club_players(
            "club-players",
            "club_players",
            th.CLUB_PLAYERS_METACOLS, th.CLUB_PLAYERS_COLS
        )
        player_profiles=transform_player_profiles(
            prefix="player-profiles",
            filename="player_profiles",
            cols=th.PLAYER_PROFILES_COLS,
        )
        player_citizenship = transform_player_citizenship(
            prefix="player-profiles",
            filename="player_citizenship"
        )
        player_other_positions = transform_player_other_positions(
            prefix="player-profiles",
            filename="player_other_positions"
        )
        player_market_values = transform_player_market_values(
            prefix="player-market-values",
            filename="player_market_values"
        )

        clubs
        club_profiles
        club_players
        player_profiles
        player_other_positions
        player_citizenship
        player_market_values

    

    @task_group(group_id="load_to_postgres_GP")
    def load_to_postgres_GP():

        load_clubs_to_gp = PostgresOperator(
            task_id="load_clubs_to_gp",
            postgres_conn_id="GP",
            sql=f"select stg.load_clubs('{th.DT1}'::date);"
            #sql="select stg.load_clubs('{{ds}}');",
            outlets=[CLUBS]
        )

        load_club_profiles_to_gp = PostgresOperator(
            task_id="load_clubs_profiles_to_gp",
            postgres_conn_id="GP",
            sql=f"select stg.load_club_profiles('{th.DT1}'::date);"
            #sql="select stg.load_club_profiles('{{ds}}');",
            outlets=[CLUBS_PROFILES]
        )

        load_club_players_to_gp = PostgresOperator(
            task_id="load_club_players_to_gp",
            postgres_conn_id="GP",
            sql=f"select stg.load_club_players('{th.DT1}'::date);"
            #sql="select stg.load_club_players('{{ds}}');",
            outlets=[CLUBS_PLAYERS]
        )

        load_player_profiles_to_gp = PostgresOperator(
            task_id="load_player_profiles_to_gp",
            postgres_conn_id="GP",
            sql=f"select stg.load_player_profiles('{th.DT1}'::date);"
            #sql="select stg.load_player_profiles('{{ds}}');",
            outlets=[PLAYER_PROFILES]
        )

        load_player_other_positions_to_gp = PostgresOperator(
            task_id="load_player_other_positions_to_gp",
            postgres_conn_id="GP",
            sql=f"select stg.load_player_other_positions('{th.DT1}'::date);"
            #sql="select stg.load_player_other_positions('{{ds}}');",
            outlets=[PLAYER_OTHER_POSITIONS]
        )

        load_player_citizenship_to_gp = PostgresOperator(
            task_id="load_player_citizenship_to_gp",
            postgres_conn_id="GP",
            sql=f"select stg.load_player_other_citizenship('{th.DT1}'::date);"
            #sql="select stg.load_player_citizenships('{{ds}}');",
            outlets=[PLAYER_CITIZENSHIP]
        )

        load_player_market_values_to_gp = PostgresOperator(
            task_id="load_player_market_values_to_gp",
            postgres_conn_id="GP",
            sql=f"select stg.load_player_market_values('{th.DT1}'::date);"
            #sql="select stg.load_player_market_values('{{ds}}');",
            outlets=[PLAYER_MARKET_VALUES]
        )
    
    start_loading >> load_clubs >> [load_club_profiles, load_club_players] >> \
    load_player_profiles >> load_player_market_values >> api_loaded >> transform_to_parquet() >> \
    load_to_postgres_GP() >> finish_loading
