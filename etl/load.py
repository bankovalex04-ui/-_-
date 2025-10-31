from sqlalchemy import create_engine
import os
from dotenv import load_dotenv


def load_data(data, max_rows=100):

    data_to_load = data.head(max_rows)

    os.makedirs("data/processed", exist_ok=True)
    data_to_load.to_parquet("data/processed/properties_processed.parquet", index=False)
    print("Датасет сохранен в data/processed/properties_processed.parquet")

    load_dotenv()

    url = os.getenv("DB_URL")
    port = os.getenv("DB_PORT")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")

    connection_string = f"postgresql://{user}:{password}@{url}:{port}/homeworks"
    engine = create_engine(connection_string)

    data_to_load.to_sql(name="bankov", con=engine, index=False, if_exists="replace")
