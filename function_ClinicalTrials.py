# -*- coding: utf-8 -*-
"""
Created on Thu Apr  8 14:19:55 2021

@author: majichkar
"""

import requests
import json
import pandas as pd
import datetime
import numpy as np

def Clinical_Trials(exp):    
    def de_list(input_field):
        if isinstance(input_field, list):
            if len(input_field) == 0:
                return None
            elif len(input_field) == 1:
                return input_field[0]
            else:
                return '; '.join(input_field)
        else:
            return input_field
        
    extract_fields = [
        "NCTId",
        "BriefSummary",
        "BriefTitle",
        "CentralContactEMail",
        "CentralContactName",
        "CentralContactPhone",
        "CentralContactPhoneExt",
        "CentralContactRole",
        "CollaboratorClass",
        "CollaboratorName",
        "CompletionDate",
        "CompletionDateType",
        "IsFDARegulatedDevice",
        "IsFDARegulatedDrug",
        "LeadSponsorClass",
        "LeadSponsorName",
        "LimitationsAndCaveatsDescription",
        "LocationCity",
        "LocationContactEMail",
        "LocationContactName"]
    
    extract_fields2 = [
        "NCTId",
        "LocationContactPhone",
        "LocationContactRole",
        "LocationContactPhoneExt",
        "LocationCountry",
        "LocationFacility",
        "LocationState",
        "LocationStatus",
        "LocationZip",
        "NCTIdAlias",
        "OfficialTitle",
        "OrgClass",
        "OrgFullName",
        "OrgStudyId",
        "OrgStudyIdDomain",
        "OrgStudyIdLink",
        "OrgStudyIdType",
        "OversightHasDMC",
        ]
    
    extract_fields3 = [
        "NCTId",
        "PatientRegistry",
        "Phase",
        "PointOfContactEMail",
        "PointOfContactOrganization",
        "PointOfContactPhone",
        "PointOfContactPhoneExt",
        "PointOfContactTitle",
        "ReferencePMID",
        "ResponsiblePartyInvestigatorAffiliation",
        "ResponsiblePartyInvestigatorFullName",
        "ResponsiblePartyInvestigatorTitle",
        "OverallOfficialName"
        ]
    
    data=pd.DataFrame()
    data1=pd.DataFrame()
    data2=pd.DataFrame()
    minrnk = 1
    maxrnk = 1000
    # exp = '("Vasomotor Symptoms")OR("Menopausal Symptoms")OR("Hot Flashes")OR("Hot Flushes")OR(Menopause)'
    BASE_URL = 'https://clinicaltrials.gov/api/query/study_fields?expr='+str(exp)+'&min_rnk='+str(minrnk)+'&max_rnk='+str(maxrnk)+'&fmt=json'
    
    query_url = f'{BASE_URL}&fields={",".join(extract_fields)}'
    #print(query_url)
    print('fetching Clinical Trials data...\n')
    print('Query -',exp)
    r = requests.get(query_url)
    r.status_code
    # query_url = f'{BASE_URL}'
    # print(query_url)
    while(r.status_code == 200):
        BASE_URL = 'https://clinicaltrials.gov/api/query/study_fields?expr='+exp+'&min_rnk='+str(minrnk)+'&max_rnk='+str(maxrnk)+'&fmt=json'
        query_url = f'{BASE_URL}&fields={",".join(extract_fields)}'
        query_url2 = f'{BASE_URL}&fields={",".join(extract_fields2)}'
        query_url3 = f'{BASE_URL}&fields={",".join(extract_fields3)}'
        r = requests.get(query_url)   
        r.status_code    
        j = json.loads(r.content)
        # df = pd.DataFrame(j['FullStudiesResponse']['FullStudies'])
        df = pd.DataFrame(j['StudyFieldsResponse']['StudyFields'])   
        for c in df.columns:
            df[c] = df[c].apply(de_list)
        df['CompletionDate'] = pd.to_datetime(df['CompletionDate'])
        df = df.sort_values(by='CompletionDate', ascending=False)    
        data = data.append(df)
        
        r = requests.get(query_url2)   
        r.status_code    
        j = json.loads(r.content)
        # df = pd.DataFrame(j['FullStudiesResponse']['FullStudies'])
        df = pd.DataFrame(j['StudyFieldsResponse']['StudyFields'])   
        for c in df.columns:
            df[c] = df[c].apply(de_list)
           
        data1 = data1.append(df)
        
        r = requests.get(query_url3)   
        r.status_code    
        j = json.loads(r.content)
        # df = pd.DataFrame(j['FullStudiesResponse']['FullStudies'])
        df = pd.DataFrame(j['StudyFieldsResponse']['StudyFields'])   
        for c in df.columns:
            df[c] = df[c].apply(de_list)
         
        data2 = data2.append(df)
        
        minrnk +=1000
        maxrnk +=1000
        #print(minrnk)
        if(j['StudyFieldsResponse']['NStudiesFound'] < minrnk):
            break
    
    final_data = data.merge(data1,on='NCTId').merge(data2,on='NCTId')
    
    del data,data1,data2
    
    final_data_NotNAN_US = final_data[(final_data['LocationCountry'].str.contains('United States') == True)&((final_data['CentralContactName'])+(final_data['LocationContactName'])+(final_data['OverallOfficialName']))].reset_index(drop=True).copy()
    
    #final_data_NotNAN_US.to_csv("C:\\Users\\majichkar\\Desktop\\final_data_NotNAN_US.csv")
    
    
    df_CentralContacts = final_data_NotNAN_US[['NCTId','CentralContactName']].reset_index(drop=True)
    
    df_CentralContacts['CentralContactName'] = df_CentralContacts['CentralContactName'].str.replace(",","/")
    df_CentralContacts = df_CentralContacts.assign(CentralContactName=df_CentralContacts['CentralContactName'].str.split(';')).explode('CentralContactName')
    df_CentralContacts['CentralContactName'] = df_CentralContacts['CentralContactName'].str.replace("(/).*","")
    df_CentralContacts['CentralContactName'] = df_CentralContacts['CentralContactName'].str.strip()
    df_CentralContacts.reset_index(drop=True, inplace = True)
    df_CentralContacts.drop_duplicates(inplace=True)
    
    df_LocationContacts = final_data_NotNAN_US[['NCTId','LocationContactName']].reset_index(drop=True)
    
    df_LocationContacts['LocationContactName'] = df_LocationContacts['LocationContactName'].str.replace(",","/")
    df_LocationContacts = df_LocationContacts.assign(LocationContactName=df_LocationContacts['LocationContactName'].str.split(';')).explode('LocationContactName')
    df_LocationContacts['LocationContactName'] = df_LocationContacts['LocationContactName'].str.replace("(/).*","")
    
    df_LocationContacts['LocationContactName'] = df_LocationContacts['LocationContactName'].str.strip()
    df_LocationContacts.drop_duplicates(inplace=True)
    df_LocationContacts = df_LocationContacts.reset_index(drop=True)
    
    df_OtherInv = final_data_NotNAN_US[['NCTId','ResponsiblePartyInvestigatorFullName','OverallOfficialName']].reset_index(drop=True)
    df_OtherInv = df_OtherInv.fillna('')
    df_OtherInv['OtherInvName'] = df_OtherInv['ResponsiblePartyInvestigatorFullName'].str.cat(df_OtherInv[['OverallOfficialName']].values,sep=';')
    df_OtherInv = df_OtherInv[['NCTId','OtherInvName']]
    
    df_OtherInv['OtherInvName'] = df_OtherInv['OtherInvName'].str.replace(",","/")
    df_OtherInv = df_OtherInv.assign(OtherInvName=df_OtherInv['OtherInvName'].str.split(';')).explode('OtherInvName')
    df_OtherInv['OtherInvName'] = df_OtherInv['OtherInvName'].str.replace("(/).*","")
    df_OtherInv['OtherInvName'] = df_OtherInv['OtherInvName'].str.strip()
    df_OtherInv.drop_duplicates(inplace=True)
    df_OtherInv = df_OtherInv[df_OtherInv['OtherInvName'] != ''].reset_index(drop=True)
    
    df_RefPMID = final_data_NotNAN_US[['NCTId','ReferencePMID']].reset_index(drop=True)
    
    df_RefPMID = df_RefPMID.assign(ReferencePMID=df_RefPMID['ReferencePMID'].str.split(';')).explode('ReferencePMID')
    df_RefPMID['ReferencePMID'] = df_RefPMID['ReferencePMID'].str.strip()
    df_RefPMID.drop_duplicates(inplace=True)
    df_RefPMID = df_RefPMID[np.logical_not(df_RefPMID['ReferencePMID'].isnull())].reset_index(drop=True)
    
    return df_CentralContacts,df_LocationContacts,df_OtherInv,df_RefPMID



def fetch_CT_npi(df_CentralContacts,df_LocationContacts,df_OtherInv):
    df_CentralContacts.rename(columns = {'CentralContactName':'Name'}, inplace = True)
    df_LocationContacts.rename(columns = {'LocationContactName':'Name'}, inplace = True)
    df_OtherInv.rename(columns = {'OtherInvName':'Name'}, inplace = True)
    
    df_CT = df_CentralContacts.append(df_LocationContacts).append(df_OtherInv).reset_index(drop=True)
    
    df_CT['firstname'] = df_CT['Name'].str.split(' ',1).str[0]
    df_CT['lastname'] = df_CT['Name'].str.split(' ').str[-1]
    
    df_CT_no_dup = df_CT[['firstname','lastname']].reset_index(drop=True)
    df_CT_no_dup.drop_duplicates(['firstname','lastname'],inplace=True)

    df = df_CT_no_dup.copy()
    df.head(5)

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
            d = pd.DataFrame.from_dict(data['results'], dtype=np.object)  
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

    df = df_CT.copy()
    df.drop_duplicates(['NCTId','firstname','lastname'],inplace=True)
    df['fullName'] = df['firstname']+' '+df['lastname']
    df['joinkey'] = df['Name']
    df['joinkey'] = df['joinkey'].str.strip()
    df['joinkey'] = df['joinkey'].str.upper()
    df.head(10)
    
    npi_df['joinkey'] = npi_df['Provider_First_Name']+' '+npi_df['Provider_Middle_Name'].replace('none','-').str[0]+' '+npi_df['Provider_Last_Name_(Legal_Name)']
    npi_df['joinkey'] = npi_df['joinkey'].str.replace(' - ',' ')
    npi_df['joinkey'] = npi_df['joinkey'].str.strip()
    npi_df['joinkey'] = npi_df['joinkey'].str.upper()
    
    CT_HCP = df.merge(npi_df,how='left',on='joinkey')
    
    CT_HCP_Not_Joined = CT_HCP[CT_HCP['NPI'].isnull()].filter(df.columns)
    CT_HCP = CT_HCP[np.logical_not(CT_HCP['NPI'].isnull())]
    
    CT_HCP['key'] = 1
    duplicates = CT_HCP[['joinkey','key']].groupby(['joinkey']).sum()
    CT_HCP.drop(['key'],inplace=True,axis=1)
    CT_HCP = CT_HCP.merge(duplicates,on='joinkey',how='inner')
    
    npi_df['joinkey'] = npi_df['Provider_First_Name']+' '+npi_df['Provider_Last_Name_(Legal_Name)']
    npi_df['joinkey'] = npi_df['joinkey'].str.strip()
    npi_df['joinkey'] = npi_df['joinkey'].str.upper()
    
    
    CT_HCP_Not_Joined['joinkey'] = CT_HCP_Not_Joined['fullName']
    CT_HCP_Not_Joined['joinkey'] = CT_HCP_Not_Joined['joinkey'].str.strip()
    CT_HCP_Not_Joined['joinkey'] = CT_HCP_Not_Joined['joinkey'].str.upper()
    CT_HCP_Not_Joined.head(10)
    
    CT_HCP_1 = CT_HCP_Not_Joined.merge(npi_df,how='left',on='joinkey')
    
    CT_HCP_Not_Joined = CT_HCP_1[CT_HCP_1['NPI'].isnull()].filter(df.columns)
    CT_HCP_1 = CT_HCP_1[np.logical_not(CT_HCP_1['NPI'].isnull())]
    CT_HCP_Not_Joined.head(1)
    
    CT_HCP_1['key'] = 1
    duplicates = CT_HCP_1[['joinkey','key']].groupby(['joinkey']).sum()
    CT_HCP_1.drop(['key'],inplace=True,axis=1)
    CT_HCP_1 = CT_HCP_1.merge(duplicates,on='joinkey',how='inner')
    
    CT_HCP = CT_HCP.append(CT_HCP_1)
    
    CT_HCP = CT_HCP.filter(['NCTId','firstname','lastname','fullName','Name','NPI','key'])
    CT_HCP_Not_Joined = CT_HCP_Not_Joined.filter(['NCTId','firstname','lastname','fullName','Name'])
    CT_HCP_Not_Joined['key'] = 0
    
    CT_HCP = CT_HCP.append(CT_HCP_Not_Joined)
    CT_HCP.reset_index(drop=True,inplace=True)
    del CT_HCP_1,CT_HCP_Not_Joined,d
    
    return CT_HCP,npi_df,df_CT











