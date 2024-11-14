from prefect.server.schemas.schedules import IntervalSchedule
from datetime import timedelta
from prefect import flow
import os
import dotenv

dotenv.load_dotenv()

SOURCE_REPO = os.getenv("SOURCE_REPO")
RAW_DIR = os.getenv("RAW_DIR")

if __name__ == "__main__":
    pulled_flow = flow.from_source(
        source=SOURCE_REPO,
        entrypoint="main.py:etl_flow"
    )
    pulled_flow.deploy(
        name="my-first-deployment",
        work_pool_name="default-work-pool",
        schedule=IntervalSchedule(interval=timedelta(days=1))
    )