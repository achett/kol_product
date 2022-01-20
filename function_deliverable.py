# -*- coding: utf-8 -*-
"""
Created on Tue Jan 11 11:09:15 2022

@author: akumar3
"""
import pandas as pd
from functools import reduce
import re

def fix_column_names(x):
    x = ''.join(re.findall(r'\w+', ' '.join(x)))
    x = x.lower()
    return x

def deliverable(path):
    
    print('creating deliverable file')
    
    pm = pd.read_csv(path+'intermediate/pubmed_data.csv')
    npi = pd.read_csv(path+"intermediate/npi_df.csv", dtype = {'NPI': 'object'})
    ct = pd.read_csv(path+'intermediate/clinicaltrials_hcp_list.csv')
    
    #format column names
    npi.columns = [fix_column_names(cols) for cols in npi.columns.tolist()]
    ct.columns = [fix_column_names(cols) for cols in ct.columns.tolist()]
    pm.columns = [fix_column_names(cols) for cols in pm.columns.tolist()]

    #convert object to date var
    npi['last_update_date'] = pd.to_datetime(npi['last_update_date'])

    #filter records from and above date jan 2018 - npi
    npi = npi[npi['last_update_date']>='2018-01-01']

    #keep required columns - npi
    npi = npi[["npi","provider_last_name_legal_name","provider_first_name","provider_middle_name", "provider_credential_text",
         "authorized_official_telephone_number","provider_first_line_business_mailing_address", "provider_second_line_business_mailing_address",
         "provider_business_mailing_address_city_name","provider_business_mailing_address_state_name","provider_business_mailing_address_postal_code",
         "provider_business_mailing_address_telephone_number"]]

    #rename columns - npi
    npi = npi.rename(columns={'provider_last_name_legal_name':'lname', "provider_first_name" : 'fname',"provider_middle_name" : 'mname',
                        "provider_credential_text" : 'cred', "authorized_official_telephone_number" : 'phone',
                        "provider_first_line_business_mailing_address" : 'address1',
                        "provider_second_line_business_mailing_address" : 'address2',
                        "provider_business_mailing_address_city_name" : 'city',"provider_business_mailing_address_state_name":'state',
                        "provider_business_mailing_address_postal_code" : 'zip',
                        "provider_business_mailing_address_telephone_number":'phone2'})

    #fetch and rename columns for clinical trials
    ct = ct[['nctid', 'firstname', 'lastname']].rename(columns={'firstname':'fname', 'lastname':'lname'})

    #fetch and name columns for pubmed
    pm = pm[["pmid","doi","title","abstract","year","month","day","jabbrv","journal","keywords","lastname","firstname","address","email"]].rename(columns={"lastname" :'lname',"firstname" : 'fname'})

    #create id column from first & last name - npi
    npi['id'] = npi['lname'].str.lower()+npi['fname'].str[:5].str.lower()

    #create id column from first & last name - clinical trial
    ct['id'] = ct['lname'].str.lower()+ct['fname'].str[:5].str.lower()

    #create count column for clinical trials based on nctid
    trials_n = ct.groupby(['id'])['nctid'].count().reset_index(name='trials_n')
    trials = ct.groupby('id')['nctid'].apply(', '.join).reset_index().rename(columns={'nctid':'trials'})
    dfs=[ct, trials, trials_n]
    ct2 = reduce(lambda left,right: pd.merge(left,right,how='outer', on='id'), dfs)
    ct2 = ct2[["id", "fname","lname", "trials", "trials_n"]]
    ct2 = ct2.drop_duplicates()

    #NPI
    #filter npi records with MD, DO
    npi_ct = npi[npi['cred'].str.contains(r'MD|M.D.|DO|D.O.')]

    #exclude npi DMD records
    npi_ct = npi_ct[~npi_ct['cred'].str.contains(r'DMD')]

    #create q_ct df with ct2 and npi_ct
    q_ct = pd.merge(ct2, npi_ct, on='id', how='left')

    #filter out empty npi records from q_ct df
    q_ct2 = q_ct[~q_ct.npi.isna()]

    #create count column for clinical trial based on npi
    q_ = q_ct2[['id', 'fname_x', 'lname_x', 'trials', 'state', 'npi']]
    trials_n = q_.groupby(['id'])['npi'].count().reset_index(name='trials_n')
    npis = q_.groupby('id')['npi'].apply(', '.join).reset_index().rename(columns={'npi':'npis'})
    states = q_.groupby('id')['state'].apply(', '.join).reset_index().rename(columns={'state':'states'})
    dfs = [q_, trials_n, states, npis]
    ct_final = reduce(lambda left,right: pd.merge(left,right,how='outer', on='id'), dfs)
    ct_final = ct_final[['id', 'fname_x', 'lname_x', 'trials', 'states', 'npis', 'trials_n']].rename(
        columns={'id':'ID', 'fname_x':'First Name', 'lname_x': 'Last Name', 'trials': 'NCTIDs', 'states':'States', 'npis':'NPIs', 'trials_n': '# of Trials'})

    ct_final = ct_final.drop_duplicates()

    #merge PM with NPI
    #create id column for pubmed data
    pm['id'] = pm['lname'].str.lower()+pm['fname'].str[:5].str.lower()

    #filter US & USA records for pubmed
    pm = pm[pm['address'].str.contains(r'US|USA|U.S.|U.S.A')]

    #fetch first name
    pm['fname'] = pm['fname'].str.split().str.get(0)

    #create count for pubmed records based on pmid
    pubs_n = pm.groupby(['id'])['pmid'].count().reset_index(name='pubs_n')
    pubs = pm.groupby('id')['pmid'].apply(', '.join).reset_index().rename(columns={'pmid':'pubs'})
    dfs=[pm, pubs_n, pubs]
    pm2 = reduce(lambda left,right: pd.merge(left,right,how='outer', on='id'), dfs)
    pm2 = pm2[["id", "fname","lname", "pubs", "pubs_n"]].drop_duplicates()

    #join pubmed with npi_ct
    q_pm = pd.merge(pm2, npi_ct, on='id', how='left')

    #filter out npi empty records
    q_pm2 = q_pm[~q_pm.npi.isna()]

    #create count for npi records based on npi
    pm_final = q_pm2[['id', 'fname_x', 'lname_x', 'pubs', 'state', 'npi']]
    pm_n = pm_final.groupby(['id'])['npi'].count().reset_index(name='n')
    pm_npis = pm_final.groupby('id')['npi'].apply(', '.join).reset_index().rename(columns={'npi':'npis'})
    pm_states = pm_final.groupby('id')['state'].apply(', '.join).reset_index().rename(columns={'state':'states'})
    dfs=[pm_final, pm_states, pm_npis, pm_n]
    pm_final = reduce(lambda left,right: pd.merge(left,right,how='outer', on='id'), dfs)
    pm_final = pm_final[['id', 'fname_x', 'lname_x', 'pubs', 'states', 'npis', 'n']].rename(
        columns={'id':'ID', 'fname_x':'First Name', 'lname_x': 'Last Name', 'pubs':'PMIDs', 'states': 'States', 'npis':'NPIs', 'n':'# of Pubs'})

    pm_final = pm_final.drop_duplicates()

    #merge pm_final with ct_final
    final = pd.merge(pm_final, ct_final, on=['ID','First Name','Last Name','States','NPIs'], how='outer')

    #Get full address
    #create ct_final2 df from q_ct2
    ct_final2 = q_ct2[['id', 'fname_x', 'lname_x', 'trials', 'trials_n', 'npi', 'cred', 'address1','address2', 'city', 'state','zip', 'phone2']]

    #rename columns
    ct_final2 = ct_final2.rename(columns={'id':"ID",'fname_x':"First Name", 'lname_x':"Last Name",'trials':"NCTIDs",'trials_n':"# of Trials",
                              'npi':"NPI", 'cred':"Credentials", 'address1':"Address1", 'address2':"Address2",'city':'City',
                              'state':'State', 'zip':'Zip', 'phone2':'Phone'})

    ct_final2 = ct_final2.drop_duplicates()

    #create pm_final2 df from q_pm2 df
    pm_final2 = q_pm2[['id', 'fname_x', 'lname_x', 'pubs','pubs_n', 'npi', 'cred', 'address1', 'address2', 'city', 'state', 'zip', 'phone2']]

    #rename columns of pm_final2
    pm_final2 = pm_final2.rename(columns={'id':'ID', 'fname_x':'First Name', 'lname_x' : 'Last Name', 'pubs': 'PMIDs','pubs_n':'# of Pubs',
                              'npi':'NPI', 'cred':'Credentials', 'address1':'Address1', 'address2':'Address2', 'city':'City',
                              'state':'State', 'zip':'Zip', 'phone2':'Phone'})

    #create final2 df with pm_final2 & ct_final2
    final2= pd.merge(pm_final2, ct_final2, on=['ID','First Name','Last Name','NPI', "Credentials", "Address1", "Address2", "City", "State", "Zip", "Phone"], how='outer')

    #rename columns
    final2 = final2.rename(columns={'ID' : 'Unique ID', "Credentials":'Cred'})
    
    final2.to_csv(path+"final/deliverable.csv", index=False)
    
    print('done deliverable file')
    
    print('\n')
    
    print('*************Done Processing**')
    
    return final2