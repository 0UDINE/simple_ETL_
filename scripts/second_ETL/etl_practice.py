import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET
import glob

target_file = 'transformed_data.csv'
log_file = "log_file.txt"


def extracted_csv(file):
    df = pd.read_csv(file)
    return df


def extracted_json(file):
    df = pd.read_json(file, lines=True)
    return df


def extracted_xml(file):
    df = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    tree = ET.parse(file)
    root = tree.getroot()
    for car in root:
        car_model = car.find("car_model").text
        year_of_manufacture = car.find("year_of_manufacture").text
        price = float(car.find("price").text)
        fuel = car.find("fuel").text
        df = pd.concat([df, pd.DataFrame(
            [{"car_model": car_model, "year_of_manufacture": year_of_manufacture, "price": price, "fuel": fuel}])],
                       ignore_index=True)
    return df


def extract():
    df = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    for csv_file in glob.glob('*.csv'):
        try:
            if csv_file != target_file:
                if df.empty:
                    df = extracted_csv(csv_file)
                else:
                    df = pd.concat([df, extracted_csv(csv_file)], ignore_index=True)
        except Exception as e:
            logging(f"Error processing CSV file {csv_file}: {str(e)}")

    for json_file in glob.glob('*.json'):
        try:
            if df.empty:
                df = extracted_csv(json_file)
            else:
                df = pd.concat([df, extracted_json(json_file)], ignore_index=True)
        except Exception as e:
            logging(f"Error processing JSON file {json_file}: {str(e)}")

    for xml_file in glob.glob('*.xml'):
        try:
            if df.empty:
                df = extracted_csv(xml_file)
            else:
                df = pd.concat([df, extracted_xml(xml_file)], ignore_index=True)
        except Exception as e:
            logging(f"Error processing XML file {xml_file}: {str(e)}")

    return df


def transform(data):
    data['price'] = data['price'].apply(lambda x: round(x, 2))
    return data


def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file, index=False)


def logging(message):
    timestamp_format = '%Y-%m-%d-%H:%M:%S'  # Fixed: format string
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)

    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')


if __name__ == "__main__":
    # Log the initialization of the ETL process
    logging("ETL Job Started")

    try:
        # Log the beginning of the Extraction process
        logging("Extract phase Started")
        extracted_data = extract()

        # Log the completion of the Extraction process
        logging("Extract phase Ended")

        if not extracted_data.empty:
            # Log the beginning of the Transformation process
            logging("Transform phase Started")
            transformed_data = transform(extracted_data)
            print("Transformed Data")
            print(transformed_data)

            # Log the completion of the Transformation process
            logging("Transform phase Ended")

            # Log the beginning of the Loading process
            logging("Load phase Started")
            load_data(target_file, transformed_data)

            # Log the completion of the Loading process
            logging("Load phase Ended")

            logging(f"ETL Job Completed Successfully. Data saved to {target_file}")
        else:
            logging("No data extracted. ETL process terminated.")

    except Exception as e:
        logging(f"ETL Job Failed: {str(e)}")

    # Log the completion of the ETL process
    logging("ETL Job Ended")





