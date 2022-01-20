# -*- coding: utf-8 -*-
"""
Created on Thu Apr  8 14:02:25 2021

@author: majichkar
"""

import requests
import math 
import numpy as np
import pandas as pd
import warnings
import configparser
import sys
from function_PubMed import *
from function_ClinicalTrials import *

warnings.filterwarnings("ignore")

def parse_properties_file(file):
    config = configparser.RawConfigParser()
    config.read(file)
    return config


def initialize_vars(file,op_dir):      
    config = parse_properties_file(file)
    
    y = config['Publication Filters']['years'].strip()
    q = config['Publication Filters']['search Terms'].strip()
    e = config['Clinical Filters']['search Terms'].strip()
    
    path = op_dir + '\\'    
    # y = input("Enter Year(Seperated by comma) - ") 
    # q = input("Enter Keywords for PubMed data(Seperated by comma) - ")
    # e = input("Enter Keywords for Clinical Trials data(Seperated by comma) - ")    
    year = y.split(',')    
    q1 = q.replace(',','[TIAB] | ')
    query_arg = '('+q1+'[TIAB]) & "'    
    e1 = e.replace(',','")OR("')
    exp = '("'+e1+'")'
    # year = ['2021']
    # query_arg = '(vasomotor symptoms[TIAB] | menopausal symptom[TIAB] | hot flashes[TIAB] | hot flushes[TIAB]) & "'
    # exp = '("Vasomotor Symptoms")OR("Menopausal Symptoms")OR("Hot Flashes")OR("Hot Flushes")OR("Menopause")'
    print('Initialized Variables')
    return year,query_arg,exp,path


def process_PM_CT(year,query_arg,exp,path):
    #Fetching Data
    pubmed_df,pubmed_df_HCP_List = pubmed(year,query_arg)
    #pubmed_HCP,pubmed_npi_df = fetch_pubmed_npi(pubmed_df_HCP_List)
    
    pubmed_df_HCP_List1 = pubmed_df_HCP_List.copy()
    pubmed_df_HCP_List1.columns = pubmed_df_HCP_List1.columns.str.upper()
    
    pubmed_df_HCP_List1.to_csv(path+"intermediate/pubmed_hcp_list.csv")
    #pubmed_HCP.to_csv("Pubmed_HCP.csv")
    
    pubmed_df1 = pubmed_df.copy()
    pubmed_df1.columns = pubmed_df1.columns.str.upper()
    
    pubmed_df.to_csv(path+"intermediate/pubmed_data.csv")
    print("\n")
    #Fetching Data
    df_CentralContacts,df_LocationContacts,df_OtherInv,df_RefPMID = Clinical_Trials(exp)
    
    df_CentralContacts.rename(columns = {'CentralContactName':'fullname'}, inplace = True)
    df_LocationContacts.rename(columns = {'LocationContactName':'fullname'}, inplace = True)
    df_OtherInv.rename(columns = {'OtherInvName':'fullname'}, inplace = True)
    
    df_CT = df_CentralContacts.append(df_LocationContacts).append(df_OtherInv).reset_index(drop=True)
    print("\n")
    df_CT['firstname'] = df_CT['fullname'].str.split(' ',1).str[0]
    df_CT['lastname'] = df_CT['fullname'].str.split(' ').str[-1]
    df_CT['name'] = df_CT['firstname'] +' '+ df_CT['lastname']
    
    df_CT['name'] = df_CT['name'].str.upper()
    df_CT['name'] = df_CT['name'].str.strip()
    
    df_CT.drop_duplicates(['NCTId','name'],inplace=True)
    
    # CT_HCP,CT_npi_df,df_CT = fetch_CT_npi(df_CentralContacts,df_LocationContacts,df_OtherInv)
    # CT_HCP.to_csv("CT_HCP.csv")
    
    CT_HCP_List = df_CT.copy()
    
    CT_HCP_List.columns = CT_HCP_List.columns.str.upper()
    
    CT_HCP_List.to_csv(path+"intermediate/clinicaltrials_hcp_list.csv")
    
    #Master_npi = pubmed_npi_df.append(CT_npi_df)
    
    pubmed_df_HCP_List.rename(columns = {'fullName':'fullname'}, inplace = True)
    CT = df_CT.filter(['NCTId','firstname','lastname','fullname','name']).drop_duplicates()
    
    pubmed_df_HCP_List['name'] = pubmed_df_HCP_List['name'].str.upper()
    
    PM_CT_data = pubmed_df_HCP_List.merge(CT,how='outer',on='name',suffixes=('_PM', '_CT'))
    
    PM_CT_agg = PM_CT_data[['pmid','NCTId','name','fullname_PM','fullname_CT']].groupby('name').agg({"pmid": pd.Series.nunique,"NCTId": pd.Series.nunique,"fullname_PM":np.max,"fullname_CT":np.max})
    
    PM_CT_agg.reset_index(inplace=True)
    PM_CT_agg['fullname_PM'] = PM_CT_agg['fullname_PM'].str.upper()
    PM_CT_agg['fullname_CT'] = PM_CT_agg['fullname_CT'].str.upper()
    
    PM_CT_agg_Final = PM_CT_agg.copy()
    PM_CT_agg.rename(columns = {'name':'HCP_Name','pmid':'Count of Publications','NCTId':'Count of Clinical Trials','fullname_PM':'Name(PubMed)','fullname_CT':'Name(ClinicalTrials)'}, inplace = True)
    
    # PM_CT_agg.to_csv("PM_CT_agg.csv")
    
    print("fetched ",len(PM_CT_agg_Final), " HCPs")
    
    #PM_CT_agg_Final.drop(columns = ['fullname_PM','fullname_CT'],inplace=True)
    return PM_CT_agg_Final, CT_HCP_List, pubmed_df

def fetch_npi(Final_df):
    # Final_df = PM_CT_CMS_Final.head(500)
    Final_df['firstname'] = Final_df['name'].str.split(' ',1).str[0]
    Final_df['lastname'] = Final_df['name'].str.split(' ').str[-1]
    
    df = Final_df.copy()
    df['list'] = df.apply(lambda x: (x['firstname'], x['lastname']), axis=1)
    
    df_new = pd.DataFrame()
    
    for x in df['list'].tolist():
        try:
            r = requests.get('https://npiregistry.cms.hhs.gov/api/?'+
                             str('enumeration_type=NPI-1')+'&'+
                             str('version=2.1')+'&'+ #+str(x['version'])+'&'+
                             str('first_name=')+str(x[0])+'&'+
                             str('last_name=')+str(x[1])+'&'+
                             str('skip=')+str('')+'&'+str('limit=')+str(''))
            data=r.json()
            d = pd.DataFrame.from_dict(data['results'], dtype = np.object)  
            df_new = df_new.append(d)
        except:
            pass
        # d['Npi_Criteria'] = 'FN+LN'
               
    basic_col =['ein', 'organization_name', 'last_name', 'first_name', 'middle_name', 'name_prefix', 'name_suffix', 'credential',
                'last_updated', 'deactivation_reason_code', 'deactivation_date', 'reactivation_date', 'gender',
                'authorized_official_telephone_number']
    
    address_col = ['address_1', 'address_2', 'city', 'state', 'postal_code', 'country_code', 'telephone_number']
    
    taxo_col = ['license', 'state']
    
    d = df_new[['number', 'basic', 'addresses', 'taxonomies']]
    
    basic = d.basic.apply(pd.Series)
    basic_df = pd.DataFrame(columns=basic_col)
    basic_df = pd.concat([basic_df,basic]).fillna('none')
    basic_df = basic_df[basic_col]
    
    d = pd.concat([d,basic_df], axis=1)
    
    d = d.explode('addresses')
    
    addresses = d.addresses.apply(pd.Series)
    
    address_df = pd.DataFrame(columns=address_col)
    address_df = pd.concat([address_df,addresses]).fillna('none')
    address_df = address_df[address_col]
    address_df = address_df.rename(columns={'state':'address_state'})
    
    d = pd.concat([d,address_df], axis=1)
    
    d = d.explode('taxonomies')
    taxonomies = d.taxonomies.apply(pd.Series)
    
    taxo_df = pd.DataFrame(columns=taxo_col)
    taxo_df = pd.concat([taxo_df,taxonomies]).fillna('none')
    taxo_df = taxo_df[taxo_col]
    taxo_df = taxo_df.rename(columns={'state':'taxo_state'})
    
    d = pd.concat([d,taxo_df], axis=1) 
    d = d.drop(['basic', 'addresses','taxonomies'], axis=1)
    d = d.drop_duplicates()
    
    d = d.rename(
    columns={'number':'NPI', 'ein': 'Employer_Identification_Number_(EIN)', 'organization_name':'Provider_Organization_Name_(Legal_Business_Name)',
             'last_name':'Provider_Last_Name_(Legal_Name)', 'first_name': 'Provider_First_Name', 'middle_name':'Provider_Middle_Name',
             'name_prefix': 'Provider_Name_Prefix_Text', 'name_suffix': 'Provider_Name_Suffix_Text',
             'credential': 'Provider_Credential_Text', 'last_updated': 'Last_Update_Date',
             'deactivation_reason_code':'NPI_Deactivation_Reason_Code', 'deactivation_date': 'NPI_Deactivation_Date',
             'reactivation_date': 'NPI_Reactivation_Date', 'gender':'Provider_Gender_Code',
             'authorized_official_telephone_number':'Authorized_Official_Telephone_Number',
             'address_1': 'Provider_First_Line_Business_Mailing_Address', 'address_2': 'Provider_Second_Line_Business_Mailing_Address',
             'city': 'Provider_Business_Mailing_Address_City_Name',
             'address_state': 'Provider_Business_Mailing_Address_State_Name',
             'postal_code' : 'Provider_Business_Mailing_Address_Postal_Code', 'country_code' : 'Provider_Business_Mailing_Address_Country_Code_(If_outside_US)',
             'telephone_number': 'Provider_Business_Mailing_Address_Telephone_Number', 'license': 'Provider_License_Number_1',
             'taxo_state':'Provider_License_Number_State_Code_1'})
    
    npi_df = d.drop_duplicates('NPI', keep='first') 
      
    df['joinkey'] = df['name']
    df['joinkey'] = df['joinkey'].str.strip()
    df['joinkey'] = df['joinkey'].str.upper()
    df.head(10)
    
    npi_df['joinkey'] = npi_df['Provider_First_Name']+' '+npi_df['Provider_Last_Name_(Legal_Name)']
    npi_df['joinkey'] = npi_df['joinkey'].str.replace(' - ',' ')
    npi_df['joinkey'] = npi_df['joinkey'].str.strip()
    npi_df['joinkey'] = npi_df['joinkey'].str.upper()
    
    HCP = df.merge(npi_df,how='left',on='joinkey')
    
    HCP_Not_Joined = HCP[HCP['NPI'].isnull()].filter(df.columns)
    HCP = HCP[np.logical_not(HCP['NPI'].isnull())]
    
    HCP['key'] = 1
    duplicates = HCP[['joinkey','key']].groupby(['joinkey']).sum()
    HCP.drop(['key'],inplace=True,axis=1)
    HCP = HCP.merge(duplicates,on='joinkey',how='inner')

    HCP_Not_Joined['key'] = 0        
    HCP = HCP.append(HCP_Not_Joined)
    
    HCP = HCP.filter(['name','NPI','key'])
    HCP.reset_index(drop=True,inplace=True)
    return HCP,npi_df


def final_process(path,PM_CT_agg_Final):
    HCP = pd.DataFrame()
    npi_df = pd.DataFrame()
    print("\n")
    for i in range(1,math.ceil(len(PM_CT_agg_Final)/100)+1):
        print(i,' fetching Npi for HCPs ',i*100-99,' to ',i*100)
        df1,df2 = fetch_npi(pd.DataFrame(PM_CT_agg_Final.loc[i*100-100:i*100,]))
        HCP = HCP.append(df1)
        npi_df = npi_df.append(df2)
        HCP.drop_duplicates(inplace=True)
        npi_df.drop_duplicates(inplace=True)
    
    HCP_agg = HCP[['name','NPI']].groupby('name').agg({"NPI": pd.Series.nunique})
    
    HCP_agg.reset_index(inplace=True)
    HCP_List = PM_CT_agg_Final.merge(HCP_agg,on='name',how='left')
    
    HCP_List = HCP_List.filter(['name', 'pmid', 'NCTId', 'NPI', 'fullname_PM', 'fullname_CT'])
    
    HCP_List['NPI'] = HCP_List['NPI'].fillna(0)
    
    HCP_List.rename(columns = {'name':'HCP_Name','pmid':'Count of Publications','NCTId':'Count of Clinical Trials','NPI':'Count of NPIs','fullname_PM':'Name(PubMed)','fullname_CT':'Name(ClinicalTrials)'}, inplace = True)
    HCP.rename(columns = {'name':'HCP_Name','key':'Count of NPIs'}, inplace = True)
    
    HCP.to_csv(path+"intermediate/hcptonpi.csv")
    HCP_List.to_csv(path+"intermediate/hcp_list.csv")
    npi_df.to_csv(path+"intermediate/npi_df.csv")
    print("\n")
    print('Done Processing')
    
    return npi_df
    





