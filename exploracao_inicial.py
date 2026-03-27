import json
import pandas as pd
 
# Carregar dados
with open("campanhas.json") as f:
    df_camp = pd.DataFrame(json.load(f))
 
with open("conversas.json") as f:
    df_conv = pd.DataFrame(json.load(f))
 

df_logs = pd.read_csv("logs_omnichannel.csv", low_memory=False)
 
campos_logs = "jsonPayload.message, severity, timestamp"
 
# Resultado
print(f"campanhas.json   : {len(df_camp):,} registros")
print(f"  campos: {', '.join(df_camp.columns)}\n")
 
print(f"conversas.json   : {len(df_conv):,} registros")
print(f"  campos: {', '.join(df_conv.columns)}\n")
 
print(f"logs_omnichannel : {len(df_logs):,} registros · logs estruturados do GKE")
print(f"  campos principais: {campos_logs}")
