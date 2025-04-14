import argparse
import io
import socket

import pandas as pd

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
    return io.StringIO(data.decode("utf-8"))


def read_csv_from_file(file_path):
    # Lê os dados do CSV de um arquivo
    with open(file_path, "r") as file:
        return io.StringIO(file.read())


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Remove todos dados que não tem a coluna "device"
    if "device" not in df.columns:
        raise ValueError("O DataFrame não contém a coluna 'device'.")
    df = df[df["device"].notna()].copy()

    df.drop(columns=["latitude", "longitude"], inplace=True)
    df.dropna(inplace=True)

    if df.empty:
        raise ValueError("O DataFrame está vazio após a normalização.")

    return df


def analyze_csv_data(csv_data):
    # Analisa os dados do CSV usando pandas
    df = pd.read_csv(csv_data, sep="|")
    df = normalize_dataframe(df)

    # Converte a coluna 'data' para datetime e cria a coluna 'ano-mes'
    df["data"] = pd.to_datetime(df["data"], format="mixed")
    df["ano-mes"] = df["data"].dt.to_period("M").astype(str)

    # Lista de sensores para análise
    sensors = ["temperatura", "umidade", "luminosidade", "ruido", "eco2", "etvoc"]

    # Calcula os valores mínimos, máximos e médios por dispositivo, ano-mês e sensor
    results = []
    for sensor in sensors:
        if sensor in df.columns:
            grouped = (
                df.groupby(["device", "ano-mes"])[sensor]
                .agg(valor_maximo="max", valor_medio="mean", valor_minimo="min")
                .reset_index()
            )
            grouped["sensor"] = sensor
            results.append(grouped)

    # Combina os resultados de todos os sensores
    final_result = pd.concat(results, ignore_index=True)

    # Reorganiza as colunas no formato solicitado
    final_result = final_result[
        ["device", "ano-mes", "sensor", "valor_maximo", "valor_medio", "valor_minimo"]
    ]

    return final_result


def process_csv_data(uds_path):
    # Lê os dados do socket e processa
    csv_data = read_csv_from_socket(uds_path)
    result = analyze_csv_data(csv_data)
    result_csv = result.to_csv(index=False)

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
        print(result.to_csv(index=False))
