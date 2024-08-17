import hashlib 
import pathlib
import pandas as pd

ROOT_DIR = pathlib.Path(__file__).parent.parent.resolve()
DATA_DIR = f"{ROOT_DIR}/data"

def generate_unique_key(*args):
    """
    Generates a unique key using the provided variables.
    Args:
        *args: Any number of variables to be used in the key generation.
    Returns:
        A unique key as a hexadecimal string.
    """
    # Concatenate string representations of all arguments
    concatenated_string = ''.join(map(str, args))
    
    # Generate a unique key using SHA-256 hash
    unique_key = hashlib.sha256(concatenated_string.encode()).hexdigest()
    
    return unique_key

# Importing Data from CSV 
def load_csv_from_data(csv_file):
    
    file_path = f"{DATA_DIR}/csv/{csv_file}.csv"

    return pd.read_csv(file_path)

# Exporting Data to CSV
def export_csv_to_data(data, filename):

    exported_dir = f"{DATA_DIR}/csv/_OUTPUT_{filename}.csv"
    
    data.to_csv(exported_dir, index=False)
    
    print(f">> Data exported to :: {exported_dir}")

    return 
