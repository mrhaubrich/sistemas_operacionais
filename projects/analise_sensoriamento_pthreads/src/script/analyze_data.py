import argparse
import io
import socket

import polars as pl

# csv:
# id|device|contagem|data|temperatura|umidade|luminosidade|ruido|eco2|etvoc|latitude|longitude


def read_csv_from_socket(uds_path):
    # Conecta ao servidor UDS e lê os dados do CSV
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client_socket:
        client_socket.connect(uds_path)
        data = b""
        while True:
            chunk = client_socket.recv(4096)
            if not chunk:
                break
            data += chunk
    return io.StringIO(data.decode("us-ascii"))


def read_csv_from_file(file_path):
    # Lê os dados do CSV de um arquivo
    with open(file_path, "r", encoding="us-ascii") as file:
        return io.StringIO(file.read())


def normalize_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    # Remove todos dados que não tem a coluna "device"
    # drop id column
    df = df.drop(["id", "latitude", "longitude"])
    df = df.drop_nulls()
    if "device" not in df.columns:
        raise ValueError("O DataFrame não contém a coluna 'device'.")
    df = df.filter(df["device"].is_not_null())

    if df.is_empty():
        raise ValueError("O DataFrame está vazio após a normalização.")

    return df


def analyze_csv_data(csv_data):
    # Analisa os dados do CSV usando polars
    df = pl.read_csv(csv_data, separator="|", infer_schema_length=10000)
    df = normalize_dataframe(df)

    # Converte a coluna 'data' para datetime e cria a coluna 'ano-mes'
    df = df.with_columns(
        df["data"]
        .str.split(" ")
        .list.get(0)
        .str.strptime(pl.Datetime, format="%Y-%m-%d")
        .alias("data")
    )
    df = df.with_columns(df["data"].dt.strftime("%Y-%m").alias("ano-mes"))

    # Lista de sensores para análise
    sensors = ["temperatura", "umidade", "luminosidade", "ruido", "eco2", "etvoc"]

    # Converta os sensores para valores numéricos
    for sensor in sensors:
        if sensor in df.columns:
            df = df.with_columns(pl.col(sensor).cast(pl.Float64))

    # Calcula os valores mínimos, máximos e médios por dispositivo, ano-mês e sensor
    results = []
    for sensor in sensors:
        if sensor in df.columns:
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

    # Combina os resultados de todos os sensores
    final_result = pl.concat(results)

    # Ordena os resultados
    final_result = final_result.sort(["device", "ano-mes", "sensor"])

    # Reorganiza as colunas no formato solicitado
    final_result = final_result.select(
        ["device", "ano-mes", "sensor", "valor_maximo", "valor_medio", "valor_minimo"]
    )

    return final_result


def process_csv_data(uds_path):
    # Lê os dados do socket e processa
    csv_data = read_csv_from_socket(uds_path)
    result = analyze_csv_data(csv_data)
    result_csv = result.write_csv()

    # Envia o CSV processado de volta para o servidor
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
