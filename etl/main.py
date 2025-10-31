import argparse
import sys
import os
from extract import extract
from transform import transform
from load import load_data
from validate import validate

sys.path.append(os.path.dirname(__file__))


def main():
    parser = argparse.ArgumentParser(description="ETL Pipeline")
    parser.add_argument("--file_id", required=True)
    parser.add_argument(
        "--load_to_db",
        action="store_true",
        help="Load dataset to database (default: False)",
    )

    args = parser.parse_args()

    raw_data = extract(args.file_id)
    transformed_data = transform(raw_data)
    is_valid = validate(transformed_data)

    if is_valid:
        print("Валидация выполнена успешно")
        if args.load_to_db:
            load_data(transformed_data)
            print("Данные загружены в БД")
        else:
            print("Данные НЕ загружены в БД")
            os.makedirs("data/processed", exist_ok=True)
            transformed_data.to_parquet(
                "data/processed/properties_processed.parquet", index=False
            )
            print("Датасет сохранен в data/processed/properties_processed.parquet")
    else:
        print("Валидация не пройдена, данные не будут обработаны")


if __name__ == "__main__":
    main()
