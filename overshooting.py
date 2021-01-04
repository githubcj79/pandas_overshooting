#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from mysql_df import mysql_df

import configparser

config = configparser.ConfigParser()
config.read('./config/app.cfg')

KM = float(config['app']['KM'])
D = int(config['app']['D'])
N_DISTANCE = int(config['app']['N_DISTANCE'])
date_query = config['app']['date_query']
hour_query = config['app']['hour_query']

str_translation = [
                        '0 - 156 mts',
                        '156 - 234 mts',
                        '234 - 546 mts',
                        '546 - 1014 mts',
                        '1.01 - 1.9 Km',
                        '1.9 - 3.5 Km',
                        '3.5 - 6.6 Km',
                        '6.6 - 14.4 Km',
                        '14.4  - 30 Km',
                        '30 - 53 Km',
                        '53 - 76 Km',
                        '76.8 - ... Km',
                    ]
num_translation = [
                        156,
                        234,
                        546,
                        1014,
                        1900,
                        3500,
                        6600,
                        14400,
                        30000,
                        53000,
                        76000,
                        100000,
                    ]

'''
ta: time advanced

leer data de sitios
eliminar sitios duplicados
copiar sitios únicos a otro df (renombrando columnas)
hacer producto cruzado de sitios
eliminar rows en que sitio y sitio_ son iguales
agregar columna distancia
---------------------------
I must calculate bearing between sites --> numpy function ...
---------------------------
Debo hacer merge entre SITE y sus celdas asociadas (cellname, azimuth)
---------------------------
Debo para cada celda, considerar los N sitios más cercanos en el rango angular y obtener la distancia promedio DP
--------------------------------
Debo para cada celda encontrar la distancia a la q se encuentra el 85% de las muestras TAP
--------------------------------
Debo calcular el overshooting (DP > TAP)
-------------------------
leer data prs: celda, L_RA_TA_UE_Index(0 - 11)
hacer funcion numpy que calcula distancia donde está el 85% de las muestras
agregar esa distancia como columna
----------------------------
entregar este df como resultado.
'''

TA_COLUMNS = None
TA_INDEX = None
SAMPLES_PERCENTAGE = 85

def ta_percentaje_distance( row ):
    # print(f'TA_COLUMNS={TA_COLUMNS}') # debug
    total = 0.0
    for i in range(1,13):
        total += row[TA_INDEX[i]]
    parcial_percentage = int(total * SAMPLES_PERCENTAGE / 100)

    parcial = 0.0
    for i in range(1,13):
        parcial += row[TA_INDEX[i]]
        if parcial >= parcial_percentage:
            return num_translation[i-1]/1000

def bearing(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = list(map(np.deg2rad,\
    [lat1, lon1, lat2, lon2]))

    dlon = lon2 - lon1

    x = np.sin(dlon) * np.cos(lat2)
    y = np.cos(lat1) * np.sin(lat2) - (np.sin(lat1)
        * np.cos(lat2) * np.cos(dlon))

    initial_bearing = np.arctan2(x, y)

    # Now we have the initial bearing but math.atan2 return values
    # from -180° to + 180° which is not what we want for a compass bearing
    # The solution is to normalize the initial bearing as shown below
    initial_bearing = np.degrees(initial_bearing)
    bearing = (initial_bearing + 360) % 360
    return bearing

def haversine(lat1, lon1, lat2, lon2):
    km_constant = 6372.795477598
    lat1, lon1, lat2, lon2 = list(map(np.deg2rad,\
    [lat1, lon1, lat2, lon2]))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) *\
    np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    mi = km_constant * c
    return mi

def rename_columns(df=None, sufix=None):

    def apply_sufix(column=None):
        return column + sufix

    columns = list(df.columns)
    map_object = map(apply_sufix, columns)
    new_columns = list(map_object)
    df.columns = new_columns

def cross_join(a_df=None, b_df=None):
    a_df['key'] = 1
    b_df['key'] = 1
    result = pd.merge(a_df, b_df, on ='key').drop("key", 1)
    return result

def show_df(df=None):
    print(df.head(30)) # debug
    print(df.columns) # debug
    print(df.shape) # debug
    print(df.dtypes) # debug
    print() # debug

def main():
    global TA_COLUMNS, TA_INDEX

    # cells_df = pd.read_csv("./data/lcellreference_v2.csv")
    # print("cells' data read", flush=True) # debug
    # ta_df = pd.read_csv("./data/prs_lte_hour_2020_12_02_v2.csv")
    # print("time advanced data read", flush=True) # debug

    query_ = ("select * from lcellreference ")
    cells_df = mysql_df(query_=query_)

    query_ = ("select * from prs_lte_hour where "
        f"(dateid_date between '{date_query}' "
        f"and '{date_query}') and dateid_hour = '{hour_query}' ")
    ta_df = mysql_df(query_=query_)

    # ---------------------------------------------
    sites_df = cells_df.copy()[['SITE', 'LAT', 'LON']]
    # print("sites' data read", flush=True) # debug
    unique_sites_df = sites_df.copy().drop_duplicates(subset=['SITE',])
    # print("duplicated sites dropped", flush=True) # debug
    unique_sites_df_ = unique_sites_df.copy()
    # print("unique sites duplicated", flush=True) # debug
    # ---------------------------------------------
    rename_columns(df=unique_sites_df_, sufix='_')
    # print("columns unique sites duplicated renamed", flush=True) # debug
    result = cross_join(a_df=unique_sites_df, b_df=unique_sites_df_)
    # print("sites cross joined", flush=True) # debug
    sites_filtered_df = result[result['SITE'] != result['SITE_']].copy()
    # print("sites cross joined filtered", flush=True) # debug
    # show_df(df=sites_filtered_df) # debug
    # ---------------------------------------------
    sites_filtered_df['distance_'] = haversine(
                                    sites_filtered_df['LAT_'].values ,
                                    sites_filtered_df['LON_'].values,
                                    sites_filtered_df['LAT'].values ,
                                    sites_filtered_df['LON'].values
                                    )
    # print("sites cross joined distance added", flush=True) # debug
    # show_df(df=sites_filtered_df) # debug
    # ---------------------------------------------
    sites_filtered_df['bearing_'] = bearing(
                                sites_filtered_df['LAT'].values ,
                                sites_filtered_df['LON'].values,
                                sites_filtered_df['LAT_'].values ,
                                sites_filtered_df['LON_'].values
                                )
    # print("sites cross joined bearing added", flush=True) # debug
    # show_df(df=sites_filtered_df) # debug
    # ---------------------------------------------
    some_data_site_df = cells_df.copy()[['SITE', 'CELLNAME', 'AZIMUTH']]
    # print("some data SITE read", flush=True) # debug
    # show_df(df=some_data_site_df) # debug
    # ---------------------------------------------
    sites_filtered_df = sites_filtered_df.drop(['LAT', 'LON', 'LAT_', 'LON_'], axis = 1).copy()
    reduce_distance_df = sites_filtered_df[sites_filtered_df['distance_'] <= KM].copy()
    site_cell_df = pd.merge(some_data_site_df, reduce_distance_df, how="inner", on=["SITE"])
    # print("some data added to SITE", flush=True) # debug
    # show_df(df=site_cell_df) # debug
    # ---------------------------------------------
    # [azimuth - D, azimuth + D] <-- bearing
    sites_filtered_df_ = site_cell_df[(site_cell_df['bearing_'] > site_cell_df['AZIMUTH'] - D) & (site_cell_df['bearing_'] < site_cell_df['AZIMUTH'] + D)].copy()
    # print("SITE_ filtered by degree range", flush=True) # debug
    # show_df(df=sites_filtered_df_) # debug
    # ---------------------------------------------
    aux_df = sites_filtered_df_.copy()[['CELLNAME', 'distance_']]
    # show_df(df=aux_df) # debug
    # ---------------------------------------------
    aux_df.sort_values(by=['CELLNAME', 'distance_'], inplace=True)
    # show_df(df=aux_df) # debug
    # ---------------------------------------------
    grouped = aux_df.groupby(['CELLNAME'])
    # print(grouped.groups) # debug
    # print(grouped.head(N_DISTANCE), flush=True) # debug
    aux2 = grouped.head(N_DISTANCE)
    # print(f'type(aux2)={type(aux2)}', flush=True) # debug
    # show_df(df=aux2) # debug
    grouped2 = aux2.groupby(['CELLNAME'])
    # print(grouped2.mean(), flush=True) # debug
    aux3 = grouped2.mean()
    # show_df(df=aux3) # debug

    aux3.reset_index(inplace=True) # great !!!
    # aux3: Index(['CELLNAME', 'distance_']
    # Index(['CELLNAME', 'distance_'], dtype='object')
    # (18239, 2)
    # CELLNAME      object
    # distance_    float64
    # dtype: object

    # show_df(df=aux3) # debug
    # df = df.rename(columns = {'index':'new column name'})

    # ---------------------------------------------
    # show_df(df=ta_df)

    ta_df_ = ta_df.copy()[['Cell_Name', 'L_RA_TA_UE_Index0',
                            'L_RA_TA_UE_Index1', 'L_RA_TA_UE_Index2',
                            'L_RA_TA_UE_Index3', 'L_RA_TA_UE_Index4',
                            'L_RA_TA_UE_Index5', 'L_RA_TA_UE_Index6',
                            'L_RA_TA_UE_Index7', 'L_RA_TA_UE_Index8',
                            'L_RA_TA_UE_Index9', 'L_RA_TA_UE_Index10',
                            'L_RA_TA_UE_Index11',
                            ]]
    # show_df(df=ta_df_) # debug

    TA_COLUMNS = list(ta_df_.columns)
    # print(f'TA_COLUMNS={TA_COLUMNS}', flush=True) # debug
    TA_INDEX = {i:f for i,f in enumerate(TA_COLUMNS)}
    # print(f'TA_INDEX={TA_INDEX}', flush=True) # debug


    ta_df_['ta_'] = ta_df_.apply(ta_percentaje_distance, axis=1) # ok
    # show_df(df=ta_df_) # debug

    aux4 = pd.merge(aux3, ta_df_, how='left', left_on='CELLNAME', right_on='Cell_Name').copy()
    # show_df(df=aux4) # debug

    # ---------------------------------------------
    # print("overshooters_", flush=True) # debug
    aux5 = aux4[aux4['distance_'] > aux4['ta_']].copy()[['CELLNAME', 'distance_', 'ta_']].copy()
    aux5.reset_index(inplace=True)
    overshooters_ = aux5.drop(['index'], axis = 1).copy()
    # show_df(df=overshooters_) # debug
    return(overshooters_)


if __name__ == '__main__':
    overshooters_ = main()
    show_df(df=overshooters_)
