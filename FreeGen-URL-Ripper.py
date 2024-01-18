import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
import io
import argparse
from urllib.parse import urlparse, urlunparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def title_case(s):
    return str(s).title() if isinstance(s, str) else s

def download_csv(session, url):
    parsed_url = urlparse(url)
    path_segments = parsed_url.path.split('/')
    new_path = '/'.join(path_segments[:-1]) + '/show_print_version.csv'
    new_query = 'locale=en'
    modified_url = urlunparse(parsed_url._replace(path=new_path, query=new_query))

    try:
        response = session.get(modified_url)
        response.raise_for_status()
        csv_data = pd.read_csv(io.StringIO(response.text))
        # Apply title case transformation to string columns
        for col in csv_data.select_dtypes(include='object').columns:
            csv_data[col] = csv_data[col].apply(title_case)
        return csv_data
    except requests.HTTPError as e:
        print(f"HTTP error occurred while downloading {modified_url}: {e}")
    except pd.errors.ParserError as e:
        print(f"Parsing error: {e} - URL: {modified_url}")
        csv_data = pd.read_csv(io.StringIO(response.text), error_bad_lines=False)
        for col in csv_data.select_dtypes(include='object').columns:
            csv_data[col] = csv_data[col].apply(title_case)
        return csv_data
    except Exception as e:
        print(f"An error occurred while downloading {modified_url}: {e}")
    return pd.DataFrame()

def create_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    })
    retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def download_and_combine_csv(input_file_path, output_csv_path, max_workers=5):
    csv_data_list = []
    url_list = []  # New list to store corresponding URLs

    # Read URLs from file and remove duplicates
    with open(input_file_path, 'r') as file:
        urls = set(url.strip() for url in file.readlines())

    session = create_session()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_csv_data = {executor.submit(download_csv, session, url): url for url in urls}
        for future in as_completed(future_to_csv_data):
            csv_data = future.result()
            if not csv_data.empty:
                csv_data_list.append(csv_data)
                url_list.append(future_to_csv_data[future])  # Store the URL

    combined_csv = pd.concat(csv_data_list, ignore_index=True)
    combined_csv['URL'] = url_list  # Add the URL column
    combined_csv.to_csv(output_csv_path, index=False)
    print(f"Combined CSV saved to {output_csv_path}")


if __name__ == "__main__":
    # Set up the argument parser
    parser = argparse.ArgumentParser(description='Download and combine CSV files from a list of URLs.')
    parser.add_argument('input_file', type=str, help='Path to the input text file containing the URLs')
    parser.add_argument('output_file', type=str, help='Path to the output CSV file to save the combined data')
    args = parser.parse_args()

    # Call the function with command-line arguments
    download_and_combine_csv(args.input_file, args.output_file)
