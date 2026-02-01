import os
import pandas as pd

# Define the file path
file_path = '../data/'
print("Absolute path:", os.path.abspath(file_path))

def cleansing():
    csv_files = [f for f in os.listdir(file_path) if f.endswith('.csv')]

    for file in csv_files:
        df = pd.read_csv(os.path.join(file_path, file))

        if not df.empty:  # Ensure the DataFrame is not empty
            initial_row_count = len(df)
            # Filter out rows where the first column has non-digit or non-special character values
            df = df[df.iloc[:,0].apply(lambda x: str(x).isdigit())]

            final_row_count = len(df)
            rows_removed = initial_row_count - final_row_count
            print(f"{file} 파일에서 {rows_removed}개의 행이 삭제되었습니다.")

            # Save the modified DataFrame back to the CSV file
            df.to_csv(os.path.join(file_path, file), index=False)
            print(f"{file} 파일이 수정되어 저장되었습니다.")  # Confirmation message
        else:
            print(f"{file} 파일이 비어 있습니다.")

cleansing()