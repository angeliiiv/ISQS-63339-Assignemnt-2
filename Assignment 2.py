# -*- coding: utf-8 -*-
"""
Created on Sun Sep 18 15:33:32 2022

@author: angel
"""
import pandas as pd

""" reading the csv files into dataframes """
fields = pd.read_csv('fields.csv')
vendors = pd.read_csv('vendors.csv')
models = pd.read_csv('models.csv')
power = pd.read_csv('powergeneration.csv')
region = pd.read_csv('region.csv')


""" Merging the Dataframes """
#Joining region on power generation using the regioncd column as our key
complete = power.merge(region, left_on='region_cd', right_on='regioncd')

#Joining region on power generation using the regioncd column as our key
complete = complete.merge(models, left_on='model_cd', right_on='modelcd')

#Joining region on power generation using the regioncd column as our key
complete = complete.merge(fields, left_on='field', right_on='fieldtxt')

#Joining region on power generation using the regioncd column as our key
complete = complete.merge(vendors, left_on='vendor', right_on='CIK')

print('We decided to use the pandas merge function which would let us define what columns we are using for our join. We were able to minimize the loss of data and join all of the files onto powergenearion which we decided to be the base of our dataset.')


"""" Replcing NaN values in the Revenue Produced column """ 
complete['RevenueProduced'] = complete['RevenueProduced'].fillna(complete.groupby(['region_cd','model_cd'])['RevenueProduced'].transform('mean'))


""" Computaitonal Columns """
#revenue per kilowatt
complete['revenue_per_kilowatt'] = complete['RevenueProduced']/complete['kilowatt_production']

#net profit in first year
complete['first_year_profit'] = complete['RevenueProduced'] - complete['yearlyupkeep']

#maintenance costs per year
complete['annual_operation_hours'] = complete['HoursPerWeekOfOperation']*52

#maintenance_costs_per_year
complete['annual_maintenance_cost'] = complete['yearlyupkeep']/complete['annual_operation_hours']

#category for Efficiency
complete['Efficiency'] = 'low'

complete['Efficiency'][(complete['annual_maintenance_cost'] > 1) & (complete['kilowatt_production'] < 3)] = 'high'
complete['Efficiency'][(complete['annual_maintenance_cost'] < 1) & (complete['annual_maintenance_cost'] > 0.5) 
                       & (complete['kilowatt_production'] > 3) & (complete['kilowatt_production'] < 6)] = 'high'
complete['Efficiency'][(complete['kilowatt_production'] > 6) & (complete['kilowatt_production'] < 9)] = 'high'

#category for VendorQuality
complete['VendorQuality'] = 'Bad'
complete['VendorQuality'][(complete['hasroads'] == 'Yes') & (complete['haseasement']== 'Yes') & 
                          (complete['AreaOfExpertise']== 'High Capacity')] = 'Good'
complete['VendorQuality'][(complete['hasroads'] == 'Yes') & (complete['haseasement']== 'Yes') & 
                          (complete['AreaOfExpertise']== 'High Efficiency')] = 'Good'


filepath='C:\\users\\eddie\Desktop\\region_aggregate.csv'

#calculate average_revenue_per_kw by region
agg1=complete.groupby('region_name')['revenue_per_kilowatt'].mean()

#calculate annual_operation_hours by region
agg2=complete.groupby('region_name')['annual_operation_hours'].mean()

#calculaate annual_maintenance_cost by region
agg3=complete.groupby('region_name')['annual_maintenance_cost'].mean()


#merge dataframes on region_name
merged_df=pd.merge(agg1,agg2, on='region_name')
merged_df2=pd.merge(merged_df,agg3, on='region_name')

#write dataframe to csv
merged_df2.to_csv(filepath)


#filepath for high efficiency model csv
filepath2 ='C:\\Users\\Dstrawma\\OneDrive - JNJ\\MYDATA\\WFA\\TTU\\Business Intelligence\\high_efficiency_model.csv'

#create high efficiency model data frame
highefficiency_df = complete.loc[complete['Efficiency'] =='high']
highefficiency_df2 = highefficiency_df[['modelcd', 'modelname', 'installcost', 'yearlyupkeep']]

#write high efficiency dataframe to csv
highefficiency_df2.to_csv(filepath2, index=False)

#adjust vendor premium based on vendor quality
complete['new_vendor_percentage'] = complete['VendorPercent'] - .05
complete['new_vendor_percentage'][(complete['VendorQuality'] == 'Good')] = (complete['VendorPercent'] + .05)


filepath3 ='C:\\Users\\Dstrawma\\OneDrive - JNJ\\MYDATA\\WFA\\TTU\\Business Intelligence\\vendor_quality.csv'

#adjust vendor premium based on vendor quality
complete['new_vendor_percentage'] = complete['VendorPercent'] - .05
complete['new_vendor_percentage'][(complete['VendorQuality'] == 'Good')] = (complete['VendorPercent'] + .05)

#calculate avg vendor percent and new vendor percent by region, Vendor and Vendor Quality
agg4=complete.groupby(['region_name', 'Vendor', 'VendorQuality'])['VendorPercent'].mean()
agg5=complete.groupby(['region_name', 'Vendor', 'VendorQuality'])['new_vendor_percentage'].mean()

#Create vendor quality DF
vendorquality_df = pd.merge(agg4, agg5, how='outer', on = ['region_name','Vendor','VendorQuality'])

#write dataframe to csv
vendorquality_df.to_csv(filepath3)
