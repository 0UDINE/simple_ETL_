import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"


# Part 1: Data Extraction Phase

def extracted_csv(file):
    df = pd.read_csv(file)
    return df


def extracted_json(file):
    df = pd.read_json(file, lines=True)
    return df


def extracted_xml(file):
    df = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file)
    root = tree.getroot()  # Fixed: missing parentheses
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        df = pd.concat([df, pd.DataFrame([{"name": name, "height": height, "weight": weight}])], ignore_index=True)
    return df  # Fixed: indent error, this should be outside the loop


def extract():
    extracted_data = pd.DataFrame(columns=["name", "height", "weight"])  # empty df

    for file in glob.glob('*.csv'):
        if file != target_file:
            extracted_data = pd.concat([extracted_data, extracted_csv(file)], ignore_index=True)

    for file in glob.glob('*.json'):
        try:
            df = extracted_json(file)  # Fixed: removed extra argument
            extracted_data = pd.concat([extracted_data, df], ignore_index=True)
        except Exception as e:
            log_progress(f"Error processing JSON file {file}: {str(e)}")

    for file in glob.glob('*.xml'):
        try:
            xml_data = extracted_xml(file)
            extracted_data = pd.concat([extracted_data, xml_data], ignore_index=True)
        except Exception as e:
            log_progress(f"Error processing XML file {file}: {str(e)}")

    return extracted_data


# Part 2: Data Transformation Phase

def transform(data):
    '''Convert inches to meters and round off to two decimals
    1 inch is 0.0254 meters '''
    data['height'] = data['height'].apply(lambda x: round(x * 0.0254, 2))

    '''Convert pounds to kilograms and round off to two decimals 
    1 pound is 0.45359237 kilograms '''
    data['weight'] = data['weight'].apply(lambda x: round(x * 0.45359237, 2))

    return data


# Part 3: Data Loading Phase

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file, index=False)  # Added index=False to avoid unnecessary index column


# Part 4: Handling Logs Phase

def log_progress(message):
    timestamp_format = '%Y-%m-%d-%H:%M:%S'  # Fixed: format string
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)

    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')


# Part 5: Testing Phase

if __name__ == "__main__":
    # Log the initialization of the ETL process
    log_progress("ETL Job Started")

    try:
        # Log the beginning of the Extraction process
        log_progress("Extract phase Started")
        extracted_data = extract()

        # Log the completion of the Extraction process
        log_progress("Extract phase Ended")

        if not extracted_data.empty:
            # Log the beginning of the Transformation process
            log_progress("Transform phase Started")
            transformed_data = transform(extracted_data)
            print("Transformed Data")
            print(transformed_data)

            # Log the completion of the Transformation process
            log_progress("Transform phase Ended")

            # Log the beginning of the Loading process
            log_progress("Load phase Started")
            load_data(target_file, transformed_data)

            # Log the completion of the Loading process
            log_progress("Load phase Ended")

            log_progress(f"ETL Job Completed Successfully. Data saved to {target_file}")
        else:
            log_progress("No data extracted. ETL process terminated.")

    except Exception as e:
        log_progress(f"ETL Job Failed: {str(e)}")

    # Log the completion of the ETL process
    log_progress("ETL Job Ended")