import csv
from io import StringIO

import requests

from .parameters import *

####
# Open Data Endpoint Configuration
####
COUNTRY_CODE = 'IT'
CITY_NAME = 'Roma'
POLLUTANT = POLLUTANT_TYPE['PM10']
YEAR_FROM = 2020
YEAR_TO = 2020

def retrieve_csv_files():
    open_data_endpoint = 'https://fme.discomap.eea.europa.eu/fmedatastreaming/AirQualityDownload/AQData_Extract.fmw?'\
                        f'CountryCode={COUNTRY_CODE}&'\
                        f'CityName={CITY_NAME}&'\
                        f'Pollutant={POLLUTANT}&'\
                        f'Year_from={YEAR_FROM}&'\
                        f'Year_to={YEAR_TO}&'\
                        f'Station=&'\
                        f'Samplingpoint=&'\
                        f'Source=All&'\
                        f'Output=TEXT&'\
                        f'UpdateDate=&'\
                        f'TimeCoverage=Year'
    r = requests.get(open_data_endpoint)
    assert(r.status_code == HTTP_CODE_OK)

    # Transcode UTF-8 response to ASCII to ignore invalid data that could pollute received URLs
    return r.text.encode('ascii', 'ignore').decode().splitlines()

def retrieve_csv_data(csv_url):
    r = requests.get(csv_url)
    assert(r.status_code == HTTP_CODE_OK)

    # Transform HTTP response in in-memory CSV file
    f = StringIO(r.text)
    reader = csv.DictReader(f)

    samples = []
    for row in reader:
        if row['Concentration'] == '':
            continue

        samples.append({
            'value': float(row['Concentration']),
            'unit': row['UnitOfMeasurement'],
            'date': row['DatetimeBegin']
        })

    return samples

if __name__ == '__main__':
    urls = retrieve_csv_files()
    samples = retrieve_csv_data(urls[0])
    print(samples)
