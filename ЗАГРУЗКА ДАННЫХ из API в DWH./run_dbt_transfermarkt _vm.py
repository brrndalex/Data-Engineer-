"""
An example DAG that uses Cosmos to render a dbt project.
"""
import os
from datetime import datetime
from pathlib import Path
from airflow.datasets import Dataset 

from cosmos import DbtDag, ExecutionMode, ExecutionConfig, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import TestBehavior


DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
PROJECT_NAME = "transfermarkt" # Сюда нужно вставить название своего проекта в dbt

CLUBS = Dataset("greenplum://stg_clubs")
CLUBS_PROFILES = Dataset("greenplum://stg_club_profiles")
CLUBS_PLAYERS = Dataset("greenplum://stg_club_players")
PLAYER_PROFILES = Dataset("greenplum://stg_player_profiles")
PLAYER_OTHER_POSITIONS = Dataset("greenplum://stg_player_other_positions")
PLAYER_CITIZENSHIP = Dataset("greenplum://stg_player_citizenship")
PLAYER_MARKET_VALUES = Dataset("greenplum://stg_market_values")

profile_config = ProfileConfig(
    profile_name="transfermarkt", # название профиля из profiles.yml
    target_name="dev", # traget в profiles.yml
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="GP", # Название connection в Airflow
        profile_args={"schema": "public"},
    ),
)

# [START virtualenv_example]
run_dbt_transfermarkt_vm = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "transfermarkt", # путь к проекту в папке dags
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.VIRTUALENV # способ запуска VIRTUALENV
        ), 
    render_config=RenderConfig(
        test_behavior=TestBehavior.NONE,
    ),
    operator_args={
        "py_system_site_packages": False,
        "py_requirements": ["dbt-greenplum"], # Сюда нужно вставить название адаптера
        "install_deps": True,
    },
    # normal dag parameters
    schedule=[CLUBS, CLUBS_PLAYERS, CLUBS_PROFILES, PLAYER_PROFILES, PLAYER_OTHER_POSITIONS,
                PLAYER_CITIZENSHIP, PLAYER_MARKET_VALUES],
    start_date=datetime(2024, 1, 28),
    catchup=False,
    dag_id="run_dbt_transfermarkt_vm",
    default_args={"retries": 2},
)

run_dbt_transfermarkt_vm
# [END virtualenv_example]
