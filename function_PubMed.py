# -*- coding: utf-8 -*-
"""
Created on Thu Apr  8 13:51:56 2021

@author: majichkar
"""

import pandas as pd
import requests
from Bio import Entrez
import re
import numpy as np

def pubmed(year,query_arg):
    id_df = pd.DataFrame()
    
    # year = ['2013','2014','2015','2016','2017','2018','2019','2020','2021']
    print('fetching PubMed data...\n')
    for i in year:
        # query = '(vasomotor symptoms[TIAB] | menopausal symptom[TIAB] | hot flashes[TIAB] | hot flushes[TIAB]) & "'+i+'"[DP]'
        query = query_arg+i+'"[DP]'
        print('Query -',query)
        Entrez.email = 'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
        handle = Entrez.esearch(db='pubmed', 
                                sort='relevance', 
                                retmax='500',
                                retmode='xml', 
                                term=query)
        result = Entrez.read(handle)
    
        id_list = result['IdList']
        id_df = id_df.append(id_list)
        
    id_df.drop_duplicates(inplace=True) 
    id_list=list(id_df[0])
    
    ids = ','.join(id_list)
    Entrez.email = 'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
    handle = Entrez.efetch(db='pubmed',
                           retmode='xml',
                           id=ids)
    results = Entrez.read(handle)
    
    #first name, last name, email
    def get_first_last_email(json_name):    
        firstname =[]
        lastname=[]
        email=[]
        
        for author in json_name:
            
            #email
            try:
                firstname.append(author['ForeName'])
                lastname.append(author['LastName'])
                
                #len(results['PubmedArticle'][12]['MedlineCitation']['Article']['AuthorList'][0]['AffiliationInfo'])
                s = author['AffiliationInfo'][0]['Affiliation']
                email_list = re.findall('\S+@\S+', s)
                if len(email_list)>0:
                    #email = ''.join(email_list)
                    t_email = ''.join(email_list)
                    email.append(t_email)
                else:
                    email.append('none')
            except IndexError:
                email.append('none')
            
            except KeyError:
                firstname.append('none')
                lastname.append('none')
    
        return firstname, lastname, email
    
    
    #keyword:
    def get_keywords(json_key):
        len_keyList = len(json_key)
        keyword = []
        
        if len_keyList > 0:
            for key in json_key[0]:
                keyword.append(key)
            keyword='; '.join(keyword)
        else:
            keyword ='none'
        return keyword
    
    
    rows = []
    for i, key in enumerate(id_list):
        temp=[]
        temp2=[]
        
        try:
            title = results['PubmedArticle'][i]['MedlineCitation']['Article']['ArticleTitle']  
            jabbrv = results['PubmedArticle'][i]['MedlineCitation']['Article']['Journal']['ISOAbbreviation']
            journal = results['PubmedArticle'][i]['MedlineCitation']['Article']['Journal']['Title']
            pmid = results['PubmedArticle'][i]['MedlineCitation']['PMID'][0:]
            abstract = results['PubmedArticle'][i]['MedlineCitation']['Article']['Abstract']['AbstractText'][0][0:]
            year = results['PubmedArticle'][i]['MedlineCitation']['Article']['ArticleDate'][0]['Year']
            month = results['PubmedArticle'][i]['MedlineCitation']['Article']['ArticleDate'][0]['Month']
            day = results['PubmedArticle'][i]['MedlineCitation']['Article']['ArticleDate'][0]['Day']
            doi = results['PubmedArticle'][i]['MedlineCitation']['Article']['ELocationID'][0][0:]
            add = results['PubmedArticle'][i]['MedlineCitation']['Article']['AuthorList'][0]['AffiliationInfo'][0]['Affiliation']
            address = re.sub('\..*','',add)
            
            keywords = get_keywords(results['PubmedArticle'][i]['MedlineCitation']['KeywordList'])
            
            firstname, lastname, email = get_first_last_email(results['PubmedArticle'][i]['MedlineCitation']['Article']['AuthorList'][0:])
        
        except KeyError:
            abstract = 'none'
            keywords='none'
            
            
        except IndexError:
            title = 'none'
            jabbrv ='none'
            journal = 'none'
            pmid ='none'
            year = 'none'
            month= 'none'
            day= 'none'
            doi='none'
            address='none'
            keywords='none'
            firstname, lastname, email = 'none', 'none', 'none'
        
        temp.extend((pmid,doi,title, abstract, year, month, day, jabbrv, journal,keywords))
        
        temp2.extend((pmid,doi,title, abstract, year, month, day, jabbrv, journal,keywords, lastname, firstname, address, email))
        
        #firstname, lastname, email = get_first_last_email(results['PubmedArticle'][i]['MedlineCitation']['Article']['AuthorList'][0:])
        if not lastname=='none':
            for last, first, em in zip(lastname, firstname, email):
                temp1=temp.copy()
                temp1.extend((last, first, address, em))
                #temp1.append(temp)
                rows.append(temp1)
        else:
            #temp1=temp.copy()
            rows.append(temp2)
            
        pubmed_df =pd.DataFrame(rows, columns=['pmid', 'doi', 'title', 'abstract', 'year', 'month', 'day','jabbrv', 'journal',
                                               'keywords', 'lastname', 'firstname', 'address', 'email'])
        
    #pubmed_df.count()
    #pubmed_df.to_csv("C:\\Users\\majichkar\\Desktop\\pubmed.csv")
    
    pubmed_df_HCP_List = pubmed_df[['pmid','firstname','lastname']][pubmed_df['pmid']!='none']
    pubmed_df_HCP_List['fullName'] = pubmed_df_HCP_List['firstname']+' '+pubmed_df_HCP_List['lastname']
    pubmed_df_HCP_List['fullName'] = pubmed_df_HCP_List['fullName'].str.strip()
    pubmed_df_HCP_List['name'] = pubmed_df_HCP_List['firstname'].str.replace('( ).*','')+' '+pubmed_df_HCP_List['lastname']
    pubmed_df_HCP_List.drop_duplicates(inplace=True)
    pubmed_df_HCP_List.head(10)
    
    # pubmed_df_HCP_List.count()
 
    return pubmed_df,pubmed_df_HCP_List

def fetch_pubmed_npi(arg):
    # arg = pubmed_df_HCP_List.head(500)
    df = arg.copy()
    df.head(5)
    
    df['firstname_only'] = df['firstname'].str.replace('( ).*','')
    df['list'] = df.apply(lambda x: (x['firstname_only'], x['lastname']), axis=1)
    
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
            d = pd.DataFrame.from_dict(data['results'])  
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
      
    df['joinkey'] = df['fullName']
    df['joinkey'] = df['joinkey'].str.strip()
    df['joinkey'] = df['joinkey'].str.upper()
    df.head(10)
    
    npi_df['joinkey'] = npi_df['Provider_First_Name']+' '+npi_df['Provider_Middle_Name'].replace('none','-').str[0]+' '+npi_df['Provider_Last_Name_(Legal_Name)']
    npi_df['joinkey'] = npi_df['joinkey'].str.replace(' - ',' ')
    npi_df['joinkey'] = npi_df['joinkey'].str.strip()
    npi_df['joinkey'] = npi_df['joinkey'].str.upper()
    
    pubmed_HCP = df.merge(npi_df,how='left',on='joinkey')
    
    pubmed_HCP_Not_Joined = pubmed_HCP[pubmed_HCP['NPI'].isnull()].filter(df.columns)
    pubmed_HCP = pubmed_HCP[np.logical_not(pubmed_HCP['NPI'].isnull())]
    
    pubmed_HCP['key'] = 1
    duplicates = pubmed_HCP[['joinkey','key']].groupby(['joinkey']).sum()
    pubmed_HCP.drop(['key'],inplace=True,axis=1)
    pubmed_HCP = pubmed_HCP.merge(duplicates,on='joinkey',how='inner')
    
    npi_df['joinkey'] = npi_df['Provider_First_Name']+' '+npi_df['Provider_Last_Name_(Legal_Name)']
    npi_df['joinkey'] = npi_df['joinkey'].str.strip()
    npi_df['joinkey'] = npi_df['joinkey'].str.upper()
    
    
    pubmed_HCP_Not_Joined['joinkey'] = pubmed_HCP_Not_Joined['name']
    pubmed_HCP_Not_Joined['joinkey'] = pubmed_HCP_Not_Joined['joinkey'].str.strip()
    pubmed_HCP_Not_Joined['joinkey'] = pubmed_HCP_Not_Joined['joinkey'].str.upper()
    pubmed_HCP_Not_Joined.head(10)
    
    pubmed_HCP_1 = pubmed_HCP_Not_Joined.merge(npi_df,how='left',on='joinkey')
    
    pubmed_HCP_Not_Joined = pubmed_HCP_1[pubmed_HCP_1['NPI'].isnull()].filter(df.columns)
    pubmed_HCP_1 = pubmed_HCP_1[np.logical_not(pubmed_HCP_1['NPI'].isnull())]
    pubmed_HCP_Not_Joined.head(1)
    
    pubmed_HCP_1['key'] = 1
    duplicates = pubmed_HCP_1[['joinkey','key']].groupby(['joinkey']).sum()
    pubmed_HCP_1.drop(['key'],inplace=True,axis=1)
    pubmed_HCP_1 = pubmed_HCP_1.merge(duplicates,on='joinkey',how='inner')
    
    pubmed_HCP = pubmed_HCP.append(pubmed_HCP_1)
    
    pubmed_HCP = pubmed_HCP.filter(['pmid','firstname','lastname','fullName','name','NPI','key'])
    pubmed_HCP_Not_Joined = pubmed_HCP_Not_Joined.filter(['pmid','firstname','lastname','fullName','name'])
    pubmed_HCP_Not_Joined['key'] = 0
    
    pubmed_HCP = pubmed_HCP.append(pubmed_HCP_Not_Joined)
    pubmed_HCP.reset_index(drop=True,inplace=True)
    del pubmed_HCP_1,pubmed_HCP_Not_Joined,d
    return pubmed_HCP,npi_df











