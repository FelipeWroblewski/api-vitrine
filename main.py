from etl.extract import extract_data
from etl.transform import transform_indicators, transform_quality
from etl.load import insert_indicadores, insert_quality


def main():
    data = extract_data()

    df_main = transform_indicators(data)
    df_quality = transform_quality(data)

    insert_indicadores(df_main)
    insert_quality(df_quality)


if __name__ == "__main__":
    main()