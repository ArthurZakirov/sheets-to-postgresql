from prefect.client.schemas.schedules import IntervalSchedule
from datetime import timedelta
from prefect import flow
import os
import dotenv

dotenv.load_dotenv()

SOURCE_REPO = os.getenv("SOURCE_REPO")
STAGING_DIR = os.getenv("STAGING_DIR")

if __name__ == "__main__":
    pulled_flow = flow.from_source(
        source=SOURCE_REPO,
        entrypoint="src/flows/etl_flow.py:etl_flow"
    )
    pulled_flow.deploy(
        name="sheets-to-azure-elt",
        work_pool_name="default-work-pool",
        schedules=[IntervalSchedule(interval=timedelta(days=1))]
    )