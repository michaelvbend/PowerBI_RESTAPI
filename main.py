from extract.extract_data import extract_data
import load.load_datasets as datasets
import load.load_refreshhistory as history
from schedule import every, repeat, run_pending

@repeat(every(1).minute)
def main():
    # Extract the data
    extract_data()

    # Load the data into the DB
    datasets.load_data()
    history.load_data()

while True:
    run_pending()

# if __name__ == "__main__":
#     main()