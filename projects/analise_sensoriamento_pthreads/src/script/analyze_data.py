import argparse
import io
import socket
from datetime import datetime

import polars as pl

BUFFER_SIZE = 65536  # 64 KiB is a good default for socket reads

# csv:
# id|device|contagem|data|temperatura|umidade|luminosidade|ruido|eco2|etvoc|latitude|longitude


def read_csv_from_socket(uds_path):
    now = datetime.now()
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client_socket:
        client_socket.connect(uds_path)
        buffer = io.StringIO()
        while True:
            chunk = client_socket.recv(BUFFER_SIZE)
            if not chunk:
                break
            buffer.write(chunk.decode("utf-8"))
    then = datetime.now()
    # print in seconds
    # print(f"Tempo de leitura do socket: {(then - now).total_seconds()} segundos")
    buffer.seek(0)
    return buffer


def read_csv_from_file(file_path):
    # Lê os dados do CSV de um arquivo
    with open(file_path, "r", encoding="utf-8") as file:
        return io.StringIO(file.read())


def normalize_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    # Remove todos dados que não tem a coluna "device"
    # Drop unnecessary columns in one go
    drop_cols = [col for col in ["id", "latitude", "longitude"] if col in df.columns]
    df = df.drop(drop_cols)
    df = df.drop_nulls()
    if "device" not in df.columns:
        raise ValueError("O DataFrame não contém a coluna 'device'.")
    df = df.filter(df["device"].is_not_null())
    if df.is_empty():
        raise ValueError("O DataFrame está vazio após a normalização.")
    return df


def analyze_csv_data(csv_data):
    df = pl.read_csv(csv_data, separator="|", infer_schema_length=0)
    # Drop unnecessary columns and nulls, filter device
    drop_cols = [col for col in ["id", "latitude", "longitude"] if col in df.columns]
    df = df.drop(drop_cols).drop_nulls()
    if "device" not in df.columns:
        raise ValueError("O DataFrame não contém a coluna 'device'.")
    df = df.filter(df["device"].is_not_null())
    if df.is_empty():
        raise ValueError("O DataFrame está vazio após a normalização.")
    # Date conversion and ano-mes
    df = df.with_columns(
        [
            df["data"]
            .str.split(" ")
            .list.get(0)
            .str.strptime(pl.Datetime, format="%Y-%m-%d")
            .alias("data")
        ]
    )
    df = df.with_columns([df["data"].dt.strftime("%Y-%m").alias("ano-mes")])
    # Sensor columns
    sensors = [
        col
        for col in ["temperatura", "umidade", "luminosidade", "ruido", "eco2", "etvoc"]
        if col in df.columns
    ]
    # Only cast if not already Float64
    df = df.with_columns(
        [
            pl.col(sensor).cast(pl.Float64)
            if df[sensor].dtype != pl.Float64
            else pl.col(sensor)
            for sensor in sensors
        ]
    )
    # Aggregation
    results = []
    for sensor in sensors:
        grouped = (
            df.group_by(["device", "ano-mes"])
            .agg(
                [
                    pl.max(sensor).alias("valor_maximo"),
                    pl.mean(sensor).alias("valor_medio"),
                    pl.min(sensor).alias("valor_minimo"),
                ]
            )
            .with_columns(pl.lit(sensor).alias("sensor"))
        )
        results.append(grouped)
    final_result = pl.concat(results)
    final_result = final_result.sort(["device", "ano-mes", "sensor"]).select(
        ["device", "ano-mes", "sensor", "valor_maximo", "valor_medio", "valor_minimo"]
    )
    return final_result


def process_csv_data(uds_path):
    csv_data = read_csv_from_socket(uds_path)
    result = analyze_csv_data(csv_data)
    result_csv = result.write_csv()
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client_socket:
        client_socket.connect(uds_path)
        client_socket.sendall(result_csv.encode("utf-8"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze CSV data from UDS or file.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--uds-location", type=str, help="Path to the Unix Domain Socket (UDS)."
    )
    group.add_argument(
        "--csv-location", type=str, help="Path to the CSV file for development."
    )

    args = parser.parse_args()

    if args.uds_location:
        process_csv_data(args.uds_location)
    elif args.csv_location:
        csv_data = read_csv_from_file(args.csv_location)
        result = analyze_csv_data(csv_data)
        print(result)
        # print(result.write_csv())
        print(len(result))
