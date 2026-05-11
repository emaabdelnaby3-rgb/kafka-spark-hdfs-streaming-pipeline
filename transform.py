import os
import pandas as pd

input_dir = "D:\\docker\\input_files"
output_dir = "D:\\docker\\cleaned"

os.makedirs(output_dir, exist_ok=True)

for file in os.listdir(input_dir):
    if file.endswith(".csv"):
        input_path = os.path.join(input_dir, file)

        df = pd.read_csv(input_path)

        df = df.dropna()

        output_file = file.replace(".csv", ".parquet")
        output_path = os.path.join(output_dir, output_file)

        df.to_parquet(output_path)

        print(f"{file} transformed successfully")

print("All files transformed")
