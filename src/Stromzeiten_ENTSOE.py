from entsoe import EntsoePandasClient
import pandas as pd
import matplotlib.pyplot as plt
from functools import lru_cache
import os
import time
from dotenv import load_dotenv

load_dotenv()


@lru_cache
def extract_basic_info(stardate, enddate, country_code):

    client = EntsoePandasClient(api_key=os.environ["API_KEY"])

    start = pd.Timestamp(stardate, tz='Europe/Brussels')
    end = pd.Timestamp(enddate, tz='Europe/Brussels')
    country_code_from = 'FR'  # France
    country_code_to = 'DE_LU'  # Germany-Luxembourg
    type_marketagreement_type = 'A01'

    # quering data:
    prices_forecast = client.query_day_ahead_prices(
        country_code, start=start, end=end)
    generation = client.query_generation(country_code, start=start, end=end)
    generation = generation.iloc[:, generation.columns.get_level_values(
        1) == 'Actual Aggregated']
    generation.columns = generation.columns.droplevel(level=1)
    load = client.query_load(country_code, start=start, end=end)
    return generation

'''
generation = extract_basic_info()

print(len(generation['Solar']))
print(len(generation.index))

for row, count in enumerate(generation['Solar']):
    print(row)          
'''