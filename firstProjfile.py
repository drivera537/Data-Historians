# Dependencies
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import requests
import json
from config import params, base_url

def wine_dropna(wine_data, col): 
    wine_data.dropna(subset=[col], inplace = True)
    print("wine_data size, after dropping NaN for "+ col + ": " + str(wine_data.shape))

def get_country_data(data, country): 
    ret_df = data.loc[data['country'] == country].copy()
    if ret_df.shape[0] > 0: 
        print('Record count for Country, ' + str(country) + ': ' + str(ret_df.shape))
        return ret_df
    else: 
        return None

def get_g_address(winery_geo): 

    formatted_address = winery_geo["results"][0]['formatted_address']
    formed_address_list = winery_geo["results"][0]['address_components']
    try: 
        street_number = formed_address_list[0]['long_name']
        route = formed_address_list[1]['long_name']
        locality = formed_address_list[2]['long_name']
        administrative_area_level_2 = formed_address_list[3]['long_name']
        administrative_area_level_1 = formed_address_list[4]['long_name']
        g_country = formed_address_list[5]['long_name']
        postal_code = formed_address_list[6]['long_name']
    except: 
        return None
    
    ret_data = (formatted_address, formed_address_list, 
                street_number, route, locality, administrative_area_level_2, administrative_area_level_1, g_country, postal_code)
    return ret_data

def check_match(formed_address, province, country): 
    return country != formed_address[7] or province != formed_address[5]

def write_results(out_path, content): 
    out_dir_name = os.path.dirname(out_path)

    if not os.path.exists(out_dir_name): 
        os.mkdir(out_dir_name, 511)
    file_name = os.path.split(out_path)[1]
    
    with open(os.path.join(out_dir_name, file_name), 'a') as ctx: 
        ctx.write(content)

wine_data = pd.read_csv('Data/winemag-data-130k-v2.csv')
wine_data.head()

cleaned_wine_data_ind_cntrs = wine_data.copy()
cleaned_wine_data_all_cntrs = wine_data.copy()

non_nullable_colums_for_individual_countries = [
                       'title', 
                       'price', 
                       'points',
                       'winery',  
                       'taster_name', 
                       'region_1'
                      ]

non_nullable_colums_for_all_countries = [
                       'title', 
                       'price', 
                       'points',
                       'winery',  
                       'taster_name'
                      ]

countries = list(cleaned_wine_data_ind_cntrs.country.unique())

print(countries)
print(len(countries))

for cols in non_nullable_colums_for_individual_countries: 
    wine_dropna(cleaned_wine_data_ind_cntrs, cols)
    
for cols in non_nullable_colums_for_all_countries: 
    wine_dropna(cleaned_wine_data_all_cntrs, cols)

data_columns_for_study = ['title', 'description', # Wine
                          'taster_name', 'taster_twitter_handle', 'points', 'price', # Rating
                          'variety', 'winery', # Wine Type
                          'province', 'region_1', 'country' # Area
                         ]
cleaned_wine_data_ind_cntrs = cleaned_wine_data_ind_cntrs[data_columns_for_study]
cleaned_wine_data_all_cntrs = cleaned_wine_data_all_cntrs[data_columns_for_study]

country_data = pd.DataFrame()
print(base_url)
unfound_winery = [{}]
for country in countries: 
    country_data = get_country_data(cleaned_wine_data_ind_cntrs, country)
    
    if isinstance(country_data, pd.DataFrame):
        if (country == 'US'): 
            print(country)
            country_data['latitude'] = ''
            country_data['longitude'] = ''
            country_data['g_address'] = ''
            country_data.index = pd.RangeIndex(len(country_data.index))
            address_dict = {}
            for index, row in country_data.iterrows(): 
                print(index)
                winery_address = f"{row['winery']} winery, {row['region_1']}, {row['province']}, {row['country']}"
                if not winery_address in address_dict.keys(): 
                    address_dict.update({winery_address: []})
                    params['address'] = winery_address
                    response = requests.get(base_url, params=params)
                    # print(response)
                    if response.status_code == 200:
                    # Extracting data in json format
                        winery_geo = response.json()
                        if winery_geo.get("results", []):
                            lat = winery_geo["results"][0]["geometry"]["location"]["lat"]
                            lon = winery_geo["results"][0]["geometry"]["location"]["lng"]
                        else: 
                            unfound_winery.append({'winery_address': f"{row['winery']} winery, {row['region_1']}, {row['province']}, {row['country']}"})
                            country_data.drop([index])
                            address_dict.pop(winery_address)
                            continue
                    formed_address = get_g_address(winery_geo)
                    if formed_address == None: 
                        unfound_winery.append({'winery_address': f"{row['winery']} winery, {row['region_1']}, {row['province']}, {row['country']}"})
                        country_data.drop([index])
                        address_dict.pop(winery_address)
                        continue
                    check_match_count = 0
                    is_winery_add_found = True
                    while not check_match(formed_address, row['province'], country): 
                        check_match_count += 1
                        params['address'] = f"{row['region_1']}, {row['province']}, {row['country']}"
                        response = requests.get(base_url, params=params)
                        winery_geo = response.json()
                        formed_address = get_g_address(winery_geo)
                        if check_match_count > 1: 
                            unfound_winery.append({'winery_address': f"{row['winery']} winery, {row['region_1']}, {row['province']}, {row['country']}"})
                            country_data.drop([index])
                            is_winery_add_found = False
                            address_dict.pop(winery_address)
                            break
                    if not is_winery_add_found: 
                        continue

                    formed_address = f'{formed_address[2]}~~~~{formed_address[3]}~~~~{formed_address[4]}~~~~{formed_address[5]}~~~~{formed_address[6]}~~~~{formed_address[7]}~~~~{formed_address[8]}'
                    
                    address_dict[winery_address] = [lat, lon, formed_address]
                    country_data.at[index, 'latitude'] = str(lat)
                    country_data.at[index, 'longitude'] = str(lon)
                    country_data.at[index, 'g_address'] = str(formed_address)
                    # print(f"Winery: {winery_address} ; LAT: {lat} ; LON: {lon}; Address: {formed_address}")
                else: 
                    lat = address_dict[winery_address][0]
                    lon = address_dict[winery_address][1]
                    formed_address = address_dict[winery_address][2]
                    country_data.at[index, 'latitude'] = str(lat)
                    country_data.at[index, 'longitude'] = str(lon)
                    country_data.at[index, 'g_address'] = str(formed_address)
            print(country_data.shape)
            country_data['g_address'].replace('', np.nan, inplace=True)
            wine_dropna(country_data, 'g_address')
            print(country_data.shape)
            country_data.to_excel('output.xlsx', sheet_name=country, float_format="%.12f")
    else: 
        continue
    
# print('--------------------------------------')
# for country in countries: 
#     country_data = get_country_data(cleaned_wine_data_all_cntrs, country)

print(unfound_winery)