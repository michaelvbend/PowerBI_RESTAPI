from extract.extract_data import extract_data
import load.load_datasets as datasets
import load.load_refreshhistory as history
import load.load_workspaces as workspace
from schedule import every, repeat, run_pending

@repeat(every(1).minute)
def main():
    # Extract the data
    extract_data()

    # Load the data into the DB
    workspace.load_data()
    datasets.load_data()
    history.load_data()



if __name__ == "__main__":
    while True:
        run_pending()