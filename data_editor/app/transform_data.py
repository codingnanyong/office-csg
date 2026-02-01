import pandas as pd
import os

# Define the base paths
data_base_path = 'data\\'
rst_base_path = 'rst\\'

arr = [
    ['banbury1', 'VIBIOT0001', '172.30.109.75'],
    ['banbury2', 'VIBIOT0002', '172.30.109.76'],
    ['chiller1', 'VIBIOT0003', '172.30.109.77'],
    ['chiller2', 'VIBIOT0004', '172.30.109.78'],
    ['chiller3', 'VIBIOT0005', '172.30.109.79'],
    ['chiller5', 'VIBIOT0007', '172.30.109.81'],
    ['chiller6', 'VIBIOT0008', '172.30.109.82']
]

def read_file_conditionally(file_path, device_id):
    current_list = []
    vib_list = []

    with open(file_path, 'r') as f:
        for line in f:
            split_line = line.strip().split(',')
            info, datetime_str, capture_dt, sensor_id = split_line[0], split_line[1], split_line[2], split_line[3].strip()

            date_part = datetime_str.split()[0].replace('-', '')
            time_part = datetime_str.split()[1].replace(':', '')[:6]

            new_line = [date_part, time_part, sensor_id, device_id, capture_dt] + split_line[4:]

            if sensor_id.startswith('CURIOT'):
                current_list.append(new_line)
            elif sensor_id.startswith('VIBIOT'):
                vib_list.append(new_line)
            else:
                raise ValueError(f"Unsupported sensor_id: {sensor_id}")

    current_columns = ['ymd', 'hmsf', 'sensor_id', 'device_id', 'capture_dt', 'r', 's', 't']
    vib_columns = ['ymd', 'hmsf', 'sensor_id', 'device_id', 'capture_dt', 'x', 'y', 'z', 'freq']

    df_current = pd.DataFrame(current_list, columns=current_columns) if current_list else None
    df_vib = pd.DataFrame(vib_list, columns=vib_columns) if vib_list else None

    return df_current, df_vib

def save_data(data_base_path, rst_base_path):
    abs_data_path = os.path.abspath(data_base_path)
    subfolders = [f for f in os.listdir(abs_data_path) if os.path.isdir(os.path.join(abs_data_path, f))]

    for subfolder in subfolders:
        data_folder_path = os.path.join(abs_data_path, subfolder)
        rst_folder_path = os.path.join(rst_base_path, subfolder)

        os.makedirs(rst_folder_path, exist_ok=True)

        device_info = next((info for info in arr if info[0] == subfolder), None)
        if device_info is None:
            print(f"No device info found for subfolder: {subfolder}")
            continue

        device_id = device_info[1]
        ip_address = device_info[2]

        files = os.listdir(data_folder_path)
        for file_name in files:
            file_path = os.path.join(data_folder_path, file_name)
            if os.path.isfile(file_path):
                df_current, df_vib = read_file_conditionally(file_path, device_id)

                timestamp = file_name[-12:-4]

                if df_current is not None and not df_current.empty:
                    curiot_output_file_name = f'iot-vj_{subfolder[:7]}-{ip_address}-vib_montrg-elec_current-{timestamp}0000000000000-{timestamp}1159595959599.csv'
                    curiot_output_file = os.path.join(rst_folder_path, curiot_output_file_name)
                    df_current.to_csv(curiot_output_file, index=False)
                    print(f'CURIOT data saved to {curiot_output_file}')

                if df_vib is not None and not df_vib.empty:
                    vibiot_output_file_name = f'iot-vj_{subfolder[:7]}-{ip_address}-vib_montrg-vibration-{timestamp}0000000000000-{timestamp}1159595959599.csv'
                    vibiot_output_file = os.path.join(rst_folder_path, vibiot_output_file_name)
                    df_vib.to_csv(vibiot_output_file, index=False)
                    print(f'VIBIOT data saved to {vibiot_output_file}')

save_data(data_base_path, rst_base_path)