from entsoe import EntsoePandasClient
import pandas as pd
import matplotlib.pyplot as plt
import os
import time
from dotenv import load_dotenv

load_dotenv()


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
    generation = generation.rename(columns={
                                   "Fossil Gas": "Gas", "Fossil Oil": "Oil", "Hydro Run-of-river and poundage": "Hydro"})
    generation['Non-Renewables'] = generation['Gas'] + \
        generation['Oil']+generation['Nuclear']
    generation['Renewables'] = generation['Biomass']+generation['Hydro']+generation['Solar'] + \
        generation['Waste']+generation['Wind Offshore'] + \
        generation['Wind Onshore']
    generation['Total'] = generation['Non-Renewables']+generation['Renewables']
    carbon_intensity_factors = {
        'Biomass': 230,
        'Gas': 469,
        'Oil': 16,
        'Hydro': 4,
        'Nuclear': 16,
        'Solar': 46,
        'Waste': 16,
        'Wind Offshore': 12,
        'Wind Onshore': 12,
    }
    df1= generation.assign(**carbon_intensity_factors).mul(generation).sum(1)/1000000
    generation['Carbon Intensity'] = df1
    generation.drop('Hydro Pumped Storage', inplace=True, axis=1)
    generation['Non-Renewables'] = generation['Gas'] + \
        generation['Oil']+generation['Nuclear']
    generation['Renewables'] = generation['Biomass']+generation['Hydro']+generation['Solar'] + \
        generation['Waste']+generation['Wind Offshore'] + \
        generation['Wind Onshore']
    generation['Total Generation'] = generation['Non-Renewables']+generation['Renewables']

    
    print(generation)
    return generation


extract_basic_info('20221207', '20221208', 'BE')
'''
generation = extract_basic_info()

print(len(generation['Solar']))
print(len(generation.index))

for row, count in enumerate(generation['Solar']):
    print(row)          
'''
