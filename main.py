from scratch_1 import extract_data,get_Datasets
import load_datasets as datasets
import load_refreshhistory as history

def main():
    for i in range(200):
        # Extract the data
        extract_data(get_Datasets())

        # Load the data into the DB
        datasets.load_data()
        history.load_data()

if __name__ == "__main__":
    main()