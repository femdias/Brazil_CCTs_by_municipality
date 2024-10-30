# -*- coding: utf-8 -*-

"""
This code organizes a dataset by municipality, month and program all Brazil's 
Conditional Cash Transfers. The data is extracted from Basedosdados' datalake.
Plots are made in the end of the code with some country-level statistics
"""


import polars as pl
import pandas as pd
import os
import unidecode
import matplotlib.pyplot as plt
import matplotlib
import seaborn as sns
import statsmodels.api as sm
import basedosdados as bd
import datetime


os.chdir(r'C:\Users\femdi\Documents\GitHub\CCTs')


''' Auxilio Brasil'''

#https://basedosdados.org/dataset/a9a5b35d-2b41-4be5-be07-7256e713ba53?table=3958086e-fe42-4ec1-aba5-c66e56445dad
query1 = "SELECT id_municipio, ano_referencia, mes_referencia, SUM(valor_parcela) AS Value,\
    COUNT(ano_referencia) AS Num_Benef\
    FROM `basedosdados.br_cgu_beneficios_cidadao.auxilio_brasil`\
    GROUP BY id_municipio, ano_referencia, mes_referencia"
                  
ab_data = bd.read_sql(query1, billing_project_id='base-dos-dados-felipe', 
                                  from_file=False, reauth=False)

# Columns of CCT program name
ab_data['Program'] = 'Auxilio Brasil'



''' Novo Bolsa Familia - Extracting from Basedosdados data lake'''

#https://basedosdados.org/dataset/a9a5b35d-2b41-4be5-be07-7256e713ba53?table=ab22fc2f-8fd1-4d0c-9a1d-e35491bbf8b3
query2 = "SELECT id_municipio, ano_referencia, mes_referencia, SUM(valor_parcela) AS Value,\
    COUNT(ano_referencia) AS Num_Benef\
    FROM `basedosdados.br_cgu_beneficios_cidadao.novo_bolsa_familia`\
    GROUP BY id_municipio, ano_referencia, mes_referencia"
                  
Novo_bf_df = bd.read_sql(query2, billing_project_id = 'base-dos-dados-felipe', 
                                  from_file=False, reauth=False)

# Columns of CCT program name
Novo_bf_df['Program'] = 'Novo Bolsa Familia'





''' (Old) Bolsa Familia - Extracting from Basedosdados data lake'''

#https://basedosdados.org/dataset/a9a5b35d-2b41-4be5-be07-7256e713ba53?table=ab22fc2f-8fd1-4d0c-9a1d-e35491bbf8b3
query3 = "SELECT id_municipio, ano_referencia, mes_referencia, SUM(valor_parcela) AS Value,\
    COUNT(ano_referencia) AS Num_Benef\
    FROM `basedosdados.br_cgu_beneficios_cidadao.bolsa_familia_pagamento`\
    GROUP BY id_municipio, ano_referencia, mes_referencia"
                  
Bolsa_Familia_df = bd.read_sql(query3, billing_project_id = 'base-dos-dados-felipe', 
                                  from_file=False, reauth=False)

# Columns of CCT program name
Bolsa_Familia_df['Program'] = 'Bolsa Familia'






''' Auxilio Emergencial '''

# https://basedosdados.org/dataset/a9a5b35d-2b41-4be5-be07-7256e713ba53?table=8b17ab2f-2869-4f28-991b-4203e5ef1f88
query4 = "SELECT id_municipio, ano, mes, SUM(valor_beneficio) AS Value,\
    COUNT(ano) AS Num_Benef\
    FROM `basedosdados.br_cgu_beneficios_cidadao.auxilio_emergencial`\
    GROUP BY id_municipio, ano, mes"

ae_df = bd.read_sql(query4, billing_project_id = 'base-dos-dados-felipe', 
                                  from_file=False, reauth=False)

# Renaming 
ae_df.columns = ['id_municipio', 'ano_referencia', 'mes_referencia', 'Value', 'Num_Benef']


# Columns of CCT program name
ae_df['Program'] = 'Auxilio Emergencial'






''' Stacking datasets '''
df_CCTs = pd.concat([Bolsa_Familia_df, ae_df, ab_data, Novo_bf_df], axis = 'rows')

# Date columns
df_CCTs['Date'] = pd.to_datetime(df_CCTs['ano_referencia'].astype(str) + df_CCTs['mes_referencia'].astype(str).str.zfill(2), format = '%Y%m')

# Selecting and renaming
df_CCTs = df_CCTs[['id_municipio', 'Date', 'Program', 'Value', 'Num_Benef']]
df_CCTs.columns = ['Code_Mun', 'Date', 'Program', 'Value', 'Num_Benef']





''' Adjustments! '''

# There are some detail that I believe are errors/ inconsistencies in the government
# released microdata 

# Removing date of Bolsa Familia that I think is duplicated with Auxilio Brasil
df_CCTs = df_CCTs[~((df_CCTs['Program'] == 'Bolsa Familia') & (df_CCTs['Date'] == datetime.datetime(2021, 11, 1)))]


# In Auxilio Brasil, there are months where the minimun is below where it should be!
# See law that fixed a minimum of 400 reais: https://www12.senado.leg.br/noticias/materias/2022/05/04/senado-confirma-auxilio-brasil-em-r-400-de-forma-permanente
# Adjustment: for dates before july of 2022, add 200 reais to the value \
condition = (df_CCTs['Date'] < datetime.datetime(2022, 7, 1)) & (df_CCTs['Program'] == 'Auxilio Brasil')
df_CCTs.loc[condition,'Value'] += 200 * df_CCTs.loc[condition, 'Num_Benef']


''' Saving '''
df_CCTs.to_excel('CCTs_by_Munic.xlsx', index = False)



''' Plotting Histograms for the whole country'''

df_CCTs_Brazil = df_CCTs.groupby(['Date','Program'])[['Num_Benef', 'Value']].sum().reset_index()

# Calculating Mean value
df_CCTs_Brazil['Mean_Value'] = df_CCTs_Brazil['Value'] / df_CCTs_Brazil['Num_Benef']

# Num_Benef in millions
df_CCTs_Brazil['Num_Benef'] = df_CCTs_Brazil['Num_Benef']/1000000



# Mean value
df_pivoted_mean = df_CCTs_Brazil.pivot(index='Date', columns='Program', values='Mean_Value').fillna(0)

# Ordering
df_pivoted_mean = df_pivoted_mean[['Auxilio Brasil', 'Bolsa Familia',
                                             'Auxilio Emergencial','Novo Bolsa Familia']]

ax = df_pivoted_mean.plot(kind='bar', stacked=True, color = ['blue','red','orange', 'green'], figsize=(10, 6))
plt.xlabel('Date')
plt.ylabel('Mean Value')
# Reduce the frequency of x-axis labels
ax.set_xticks(range(0, len(df_pivoted_mean), 4))
ax.set_xticklabels([df_pivoted_mean.index.strftime('%b-%y')[i] for i in range(len(df_pivoted_mean))][::4], rotation=90)
plt.tight_layout()
plt.savefig('CCTs_mean_value.png',facecolor='white', bbox_inches = 'tight', dpi = 300)
plt.show()


# Number of Beneficiaries
df_pivoted_num_benef  = df_CCTs_Brazil.pivot(index='Date', columns='Program', values='Num_Benef').fillna(0)

# Ordering
df_pivoted_num_benef = df_pivoted_num_benef[['Auxilio Brasil', 'Bolsa Familia',
                                             'Auxilio Emergencial','Novo Bolsa Familia']]

ax = df_pivoted_num_benef.plot(kind='bar', stacked=True, color = ['blue','red', 'orange','green'], figsize=(10, 6))
plt.xlabel('Date')
plt.ylabel('Number of Beneficiaries (millions)')
# Reduce the frequency of x-axis labels
ax.set_xticks(range(0, len(df_pivoted_num_benef), 4))
ax.set_xticklabels([df_pivoted_num_benef.index.strftime('%b-%y')[i] for i in range(len(df_pivoted_num_benef))][::4], rotation=90)
plt.tight_layout()
plt.savefig('CCTs_num_benef.png',facecolor='white', bbox_inches = 'tight', dpi = 300)
plt.show()



# Number of Beneficiaries without Auxilio Emergencial (and removing last month of Bolsa Familia)
all_CCT_df1 = df_CCTs_Brazil[df_CCTs_Brazil['Program'] != 'Auxilio Emergencial']
#all_CCT_df1 = all_CCT_df1[~((all_CCT_df1['Program'] == 'Bolsa Familia') & (all_CCT_df1['Date'] == datetime.datetime(2021, 11, 1)))]
df_pivoted_num_benef1  = all_CCT_df1.pivot(index='Date', columns='Program', values='Num_Benef').fillna(0)

ax = df_pivoted_num_benef1.plot(kind='bar', stacked=True, color = ['blue','red','green'], figsize=(10, 6))
plt.xlabel('Date')
plt.ylabel('Number of Beneficiaries (millions)')
#plt.ylim(0,30)
# Reduce the frequency of x-axis labels
ax.set_xticks(range(0, len(df_pivoted_num_benef1), 4))
ax.set_xticklabels([df_pivoted_num_benef1.index.strftime('%b-%y')[i] for i in range(len(df_pivoted_num_benef1))][::4], rotation=90)
plt.tight_layout()
plt.savefig('CCTs_no_AE_num_benef.png',facecolor='white', bbox_inches = 'tight', dpi = 300)
plt.show()



