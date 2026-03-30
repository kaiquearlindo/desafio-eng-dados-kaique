import json
import pandas as pd
import gcsfs
 
BUCKET = "gs://stg-lake-raw-data_governance"
fs = gcsfs.GCSFileSystem()
 
# Carregar dados direto do GCS
with fs.open(f"{BUCKET}/campanhas.json") as f:
    df_camp = pd.DataFrame(json.load(f))
 
with fs.open(f"{BUCKET}/conversas.json") as f:
    df_conv = pd.DataFrame(json.load(f))
 
with fs.open(f"{BUCKET}/logs_omnichannel.csv") as f:
    df_logs = pd.read_csv(f, low_memory=False)
 
# Campos relevantes dos logs (o CSV tem 33 colunas — exibir só os principais)
campos_logs = "jsonPayload.message, severity, timestamp"

# Resultado
print(f"campanhas.json   : {len(df_camp):,} registros")
print(f"  campos: {', '.join(df_camp.columns)}\n")
 
print(f"conversas.json   : {len(df_conv):,} registros")
print(f"  campos: {', '.join(df_conv.columns)}\n")
 
print(f"logs_omnichannel : {len(df_logs):,} registros · logs estruturados do GKE")
print(f"  campos principais: {campos_logs}")
