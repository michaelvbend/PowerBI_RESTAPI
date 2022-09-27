from extract.extract_data import extract_data
import load.load_datasets as datasets
import load.load_refreshhistory as history

def main():
    for i in range(200):
        # Extract the data
        extract_data()

        # Load the data into the DB
        datasets.load_data()
        history.load_data()



if __name__ == "__main__":
    main()