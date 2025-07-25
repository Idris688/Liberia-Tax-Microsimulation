# -*- coding: utf-8 -*-
"""
Created on Fri Jul 18 15:45:46 2025

@author: ssj34
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import sys
sys.path.insert(0, 'C:/Users/ssj34/Documents/OneDrive/python_latest/Tax-Revenue-Analysis')
from stata_python import *
path = "C:/Users/ssj34/Documents/OneDrive/python_latest/Household Surveys/Liberia/HIES Anonymous Data/"
df_weight1 = pd.read_stata(path+"2_ALLHH_A_weighted_clean..dta", convert_categoricals=False, iterator = True)
labels = df_weight1.variable_labels()
df_weight = pd.read_stata(path+"2_ALLHH_A_weighted_clean..dta", convert_categoricals=False)
#df = df.rename(columns={'hhid':'id_n'})
df_weight = df_weight[['hhid', 'weight']]
df_weight = df_weight.set_index(['hhid'])

df_food_long = pd.read_stata(path+"HH_K1.dta")
df_food_long = df_food_long.rename(columns={'hh_k_00_b':'item_code', 'hh_k_05_1':'amt', 'hh_k_05_2':'amt_usd'})
df_food_long['amt'] = df_food_long['amt'].fillna(0)
df_food_long['amt_usd'] = df_food_long['amt_usd'].fillna(0)
df_food_long['amt'] = df_food_long['amt']/94.43 + df_food_long['amt_usd']
df_food_long['amt'] = df_food_long['amt']*4*12
df_food = df_food_long.pivot(index='hhid', columns='item_code', values=['amt'])
df_food.columns = df_food.columns.droplevel(0)

df_7_day_long = pd.read_stata(path+"HH_L1A.dta")
df_7_day_long = df_7_day_long.rename(columns={'hh_l1a_00':'item_code', 'hh_l1a_02_1':'amt', 'hh_l1a_02_2':'amt_usd'})
df_7_day_long['amt'] = df_7_day_long['amt'].fillna(0)
df_7_day_long['amt_usd'] = df_7_day_long['amt_usd'].fillna(0)
df_7_day_long['amt'] = df_7_day_long['amt']/94.43 + df_7_day_long['amt_usd']
df_7_day_long['amt'] = df_7_day_long['amt']*4*12
df_7_day = df_7_day_long.pivot(index='hhid', columns='item_code', values=['amt'])
df_7_day.columns = df_7_day.columns.droplevel(0)

df_30_day_long = pd.read_stata(path+"HH_L1B.dta")
df_30_day_long = df_30_day_long.rename(columns={'hh_l1b_00':'item_code', 'hh_l1b_02_1':'amt', 'hh_l1b_02_2':'amt_usd'})
df_30_day_long['amt'] = df_30_day_long['amt'].fillna(0)
df_30_day_long['amt_usd'] = df_30_day_long['amt_usd'].fillna(0)
df_30_day_long['amt'] = df_30_day_long['amt']/94.43+df_30_day_long['amt_usd']
df_30_day_long['amt'] = df_30_day_long['amt']*12
df_30_day = df_30_day_long.pivot(index='hhid', columns='item_code', values=['amt'])
df_30_day.columns = df_30_day.columns.droplevel(0)

df_year_long = pd.read_stata(path+"HH_L2.dta")
df_year_long = df_year_long.rename(columns={'hh_l2_00':'item_code', 'hh_l2_02_1':'amt', 'hh_l2_02_2':'amt_usd'})
df_year_long['amt'] = df_year_long['amt'].fillna(0)
df_year_long['amt_usd'] = df_year_long['amt_usd'].fillna(0)
df_year_long['amt'] = df_year_long['amt']/94.43+df_year_long['amt_usd']
df_year = df_year_long.pivot(index='hhid', columns='item_code', values=['amt'])
df_year.columns = df_year.columns.droplevel(0)

df_educ_long = pd.read_stata(path+"HH_C.dta")
df_educ_long = df_educ_long.rename(columns={'hh_l2_00':'item_code', 'hh_l2_02_1':'amt', 'hh_l2_02_2':'amt_usd'})
df_educ_long['Education'] = 0
for col in ['hh_c_39_a_1','hh_c_39_b_1','hh_c_39_c_1','hh_c_39_d_1','hh_c_39_e_1','hh_c_39_f_1','hh_c_39_g_1','hh_c_39_h_1']:
    df_educ_long[col]=df_educ_long[col].fillna(0)
    df_educ_long['Education'] = df_educ_long['Education']+df_educ_long[col]
df_educ_long['Education_usd'] = 0
for col in ['hh_c_39_a_2','hh_c_39_b_2','hh_c_39_c_2','hh_c_39_d_2','hh_c_39_e_2','hh_c_39_f_2','hh_c_39_g_2','hh_c_39_h_2']:
    df_educ_long[col]=df_educ_long[col].fillna(0)
    df_educ_long['Education_usd'] = df_educ_long['Education_usd']+df_educ_long[col]  
df_educ_long['Education'] = df_educ_long['Education']/94.43 + df_educ_long['Education_usd']
df_educ_long = df_educ_long[['hhid', 'Education']]
df_educ = df_educ_long.groupby(['hhid'])['Education'].sum().reset_index()

df_health_long = pd.read_stata(path+"HH_D.dta")
df_health_long = df_health_long.rename(columns={'hh_l2_00':'item_code', 'hh_l2_02_1':'amt', 'hh_l2_02_2':'amt_usd'})
df_health_long['Health'] = 0
for col in ['hh_d_11_1', 'hh_d_15_1', 'hh_d_16_1', 'hh_d_20_1', 'hh_d_22_1']:
    df_health_long[col]=df_health_long[col].fillna(0)
    df_health_long['Health'] = df_health_long['Health']+df_health_long[col]
df_health_long['Health_usd'] = 0
for col in ['hh_d_11_2', 'hh_d_15_2', 'hh_d_16_2', 'hh_d_20_2', 'hh_d_22_2']:
    df_health_long[col]=df_health_long[col].fillna(0)
    df_health_long['Health_usd'] = df_health_long['Health_usd']+df_health_long[col]
df_health_long['Health'] = df_health_long['Health']/94.43 + df_health_long['Health_usd']
df_health_long = df_health_long[['hhid', 'Health']]
df_health = df_health_long.groupby(['hhid'])['Health'].sum().reset_index()

df_restaurant_long = pd.read_stata(path+"HH_F.dta")
df_restaurant_long = df_restaurant_long.rename(columns={'hh_l2_00':'item_code', 'hh_l2_02_1':'amt', 'hh_l2_02_2':'amt_usd'})
df_restaurant_long['Restaurant'] = 0
for col in ['hh_f_03_1', 'hh_f_05_1', 'hh_f_07_1', 'hh_f_09_1', 'hh_f_11_1', 'hh_f_13_1']:
    df_restaurant_long[col]=df_restaurant_long[col].fillna(0)
    df_restaurant_long['Restaurant'] = df_restaurant_long['Restaurant']+df_restaurant_long[col]
df_restaurant_long['Restaurant_usd'] = 0
for col in ['hh_f_03_2', 'hh_f_05_2', 'hh_f_07_2', 'hh_f_09_2', 'hh_f_11_2', 'hh_f_13_2']:
    df_restaurant_long[col]=df_restaurant_long[col].fillna(0)
    df_restaurant_long['Restaurant_usd'] = df_restaurant_long['Restaurant_usd']+df_restaurant_long[col]
df_restaurant_long['Restaurant'] = df_restaurant_long['Restaurant']/94.43 + df_restaurant_long['Restaurant_usd']
df_restaurant_long = df_restaurant_long[['hhid', 'Restaurant']]
df_restaurant = df_restaurant_long.groupby(['hhid'])['Restaurant'].sum().reset_index()

df_housing_long = pd.read_stata(path+"HH_J1.dta")
df_housing_long['Housing'] = 0
for col in ['hh_j_04_1','hh_j_06_1','hh_j_14_1']:
    df_housing_long[col]=df_housing_long[col].fillna(0)
    df_housing_long['Housing'] = df_housing_long['Housing']+df_housing_long[col]
df_housing_long['Housing_usd'] = 0
for col in ['hh_j_04_2','hh_j_06_2','hh_j_14_2']:
    df_housing_long[col]=df_housing_long[col].fillna(0)
    df_housing_long['Housing_usd'] = df_housing_long['Housing_usd']/94.43 + df_housing_long[col]
df_housing_long['Housing'] = df_housing_long['Housing']+df_housing_long['Housing']
df_housing = df_housing_long[['hhid', 'Housing']]

df_water_long = pd.read_stata(path+"HH_J2.dta")
df_water_long['hh_j_28_1'] = df_water_long['hh_j_28_1'].fillna(0)
df_water_long['Water'] = df_water_long['hh_j_28_1']
df_water_long['hh_j_28_2'] = df_water_long['hh_j_28_2'].fillna(0)
df_water_long['Water_usd'] = df_water_long['hh_j_28_2']
df_water_long['Water'] = df_water_long['Water']/94.43 + df_water_long['Water_usd']
df_water = df_water_long.groupby(['hhid'])['Water'].sum().reset_index()

df_wide = multi_merge(df_food, [df_7_day, df_30_day, df_year, df_educ, df_health, df_restaurant, df_housing, df_water], ['hhid'])

df_wide = df_wide.set_index('hhid')

df_wide1 = df_wide.join(df_weight)

df_long = df_wide.stack().reset_index()
df_long = df_long.rename(columns={'level_1':'item', 0:'amt'})

path2 = "C:/Users/ssj34/Documents/OneDrive/python_latest/Microsimulation/Liberia-Tax-Microsimulation/"

df_map = pd.read_csv(path2+'liberia_hhs_item_mapping.csv')

df_long = pd.merge(df_long, df_map, on='item', how='left')
df_long = df_long.groupby(['hhid', 'Category'])['amt'].sum().reset_index()

df = df_long.pivot(index='hhid', columns='Category', values=['amt'])
df.columns = df.columns.droplevel(0)

df.columns = ['CONS_' + col for col in df.columns]
df['CONS_Total'] = df.sum(axis=1)
df['CONS_Other'] = df['CONS_Total'] - df['CONS_Food_Non_Processed'] - df['CONS_Food_Processed']

df['Year'] = 2016
df.index.name = 'id_n'
df = df.join(df_weight)
df.to_csv(path2+'taxcalc/vat_liberia.csv')
df.to_csv(path2+'taxcalc/vat.csv')
gst_collection_2016 = 89.0 
model_gst_collection = 112.27
multiplicative_factor = gst_collection_2016/model_gst_collection
df_weight = df[['weight']]*multiplicative_factor
df_weight = df_weight.rename(columns={'weight':'WT2016'})
for year in range(2017, 2031):
    df_weight['WT'+str(year)] = df_weight['WT2016']
df_weight.to_csv(path2+'taxcalc/vat_weights_liberia.csv')
df_weight.to_csv(path2+'taxcalc/vat_weights.csv')

#df_wages = pd.read_stata("HIES Anonymous Data/HH_E.dta")    
#hh_e_25_1 hh_e_27_1 hh_e_28 hh_e_29 hh_e_30 hh_e_31 hh_e_44_1 hh_e_44_3 hh_e_46_1 hh_e_47 hh_e_48 hh_e_49 hh_e_50
# Calibration for Liberia

