import socket
import pandas as pd
import io
import sys

def process_csv_data(uds_path):
    # Connect to the UDS server
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client_socket:
        client_socket.connect(uds_path)

        # Receive the CSV data
        data = b""
        while True:
            chunk = client_socket.recv(4096)
            if not chunk:
                break
            data += chunk

        # Wrap the data in a StringIO object and parse it with pandas
        csv_data = io.StringIO(data.decode('utf-8'))
        df = pd.read_csv(csv_data)

        # Perform data analysis (example: calculate min, max, and mean)
        result = df.groupby('name').agg({'value': ['min', 'max', 'mean']})

        # Convert the result back to CSV
        result_csv = result.to_csv()

        # Send the processed CSV back to the server
        client_socket.sendall(result_csv.encode('utf-8'))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 analyze_data.py <uds_path>")
        sys.exit(1)

    uds_path = sys.argv[1]
    process_csv_data(uds_path)