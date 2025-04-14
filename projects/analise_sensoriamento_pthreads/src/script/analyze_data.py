import socket
import pandas as pd
import io
import sys

def process_csv_data(uds_path):
    # Conecta ao servidor UDS
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client_socket:
        client_socket.connect(uds_path)

        # Recebe os dados do CSV
        data = b""
        while True:
            chunk = client_socket.recv(4096)
            if not chunk:
                break
            data += chunk

        # Envolve os dados em um objeto StringIO e os analisa com pandas
        csv_data = io.StringIO(data.decode('utf-8'))
        df = pd.read_csv(csv_data)

        # Realiza a análise de dados (exemplo: calcula mínimo, máximo e média)
        result = df.groupby('name').agg({'value': ['min', 'max', 'mean']})

        # Converte o resultado de volta para o formato CSV
        result_csv = result.to_csv()

        # Envia o CSV processado de volta para o servidor
        client_socket.sendall(result_csv.encode('utf-8'))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 analyze_data.py <uds_path>")
        sys.exit(1)

    uds_path = sys.argv[1]
    process_csv_data(uds_path)