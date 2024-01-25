import requests
import re
import os

esrally_download_files_location = "./rally-download/"

def fetch_content_from_url(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")


def parse_corpora(content, number_of_loops):
    # Manually added entry for kafka-logs
    kafka_logs_entry = {
        'name': 'kafka-logs',
        'base_url': 'https://rally-tracks.elastic.co/observability/logging/kafka/kafka.log/raw',
        'target_data_stream': 'logs-kafka.log-default',
        'source_file_template': 'document-{{i}}.json.bz2'
    }

    # Process the kafka_logs_entry with the loop logic
    parsed_data = process_entry(kafka_logs_entry, number_of_loops)

    # Pattern to match each corpus block
    corpus_pattern = r'\{\s*"name":\s*"(.*?)",\s*"base-url":\s*"(.*?)",\s*"documents":\s*\[(.*?)\]\s*\}'

    for corpus_match in re.finditer(corpus_pattern, content, re.DOTALL):
        name, base_url, documents_str = corpus_match.groups()
        base_url = base_url.replace("{{p_corpora_uri_base}}", "https://rally-tracks.elastic.co")

        document_pattern = r'\{\s*"target-data-stream":\s*"(.*?)",\s*"source-file":\s*"(.*?)"'
        document_match = re.search(document_pattern, documents_str, re.DOTALL)

        if document_match:
            target_data_stream, source_file_template = document_match.groups()
            corpus_entry = {
                'name': name,
                'base_url': base_url,
                'target_data_stream': target_data_stream,
                'source_file_template': source_file_template
            }
            parsed_data += process_entry(corpus_entry, number_of_loops)

    return parsed_data


def process_entry(entry, number_of_loops):
    entry_data = []
    for i in range(number_of_loops + 1):
        source_file = entry['source_file_template'].replace("{{i}}", str(i))
        file_location = f"{entry['base_url']}/{source_file}"
        entry_data.append({
            'name': entry['name'],
            'base_url': entry['base_url'],
            'target_data_stream': entry['target_data_stream'],
            'source_file': source_file,
            'file_location': file_location
        })
    return entry_data


url = "https://raw.githubusercontent.com/elastic/rally-tracks/b812ad827479dce66521cb77966ba9c129a96749/elastic/logs/track.json"
content = fetch_content_from_url(url)

number_of_loops = 1  # Set the number of loops here

parsed_corpora = parse_corpora(content, number_of_loops)

# Remove entries 4-6 (indices 3, 4, 5)
parsed_corpora = parsed_corpora[:3] + parsed_corpora[6:]

for corpus in parsed_corpora:
    print(corpus)




import requests
import os

def download_file(url, local_filename, download_path):
    if not os.path.exists(download_path):
        os.makedirs(download_path)
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(os.path.join(download_path, local_filename), 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


# Exclude entries 4-6 (indices 3, 4, 5)
parsed_corpora = parsed_corpora[:3] + parsed_corpora[6:]

# Download each file
for corpus in parsed_corpora:
    file_url = corpus['file_location']
    local_filename = corpus['source_file'].replace("document", corpus['target_data_stream'])
    try:
        download_file(file_url, local_filename, esrally_download_files_location)
        print(f"Downloaded to {esrally_download_files_location}{local_filename}")
    except Exception as e:
        print(f"Failed to download to {esrally_download_files_location}{local_filename}. Error: {e}")
