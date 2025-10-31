import pandas as pd
import os
from validate import validate


def extract(file_id):
    file_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    raw_data = pd.read_csv(file_url)

    if validate(raw_data):
        os.makedirs("data/raw", exist_ok=True)
        raw_data.to_csv("data/raw/properties_raw.csv", index=False)

        print("Сырой датасет сохранен в data/raw/properties_raw.csv")
        return raw_data
