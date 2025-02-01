import os

# Get the absolute path to the project root
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Construct the path to the data directory
data_dir = os.path.join(base_dir, "data")

# Construct the paths to the data files
ORDERS_FILE = os.path.join(data_dir, "orders.csv")
INVOICING_FILE = os.path.join(data_dir, "invoicing_data.json")
OUTPUT_FOLDER = os.path.join(data_dir, "output/crate_distribution")
