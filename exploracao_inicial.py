"""
exploracao_inicial.py
---------------------
Análise exploratória local dos dados antes da ingestão no BigQuery.
Executada para formular hipóteses e entender a estrutura das tabelas.

Dependências: pandas
"""

import json
import pandas as pd

# ============================================================
# CARREGAR DADOS
# ============================================================

with open("campanhas.json") as f:
    df_camp = pd.DataFrame(json.load(f))

with open("conversas.json") as f:
    df_conv = pd.DataFrame(json.load(f))

df_logs = pd.read_csv("logs_omnichannel.csv")

print("=== SHAPE ===")
print(f"campanhas  : {df_camp.shape}")
print(f"conversas  : {df_conv.shape}")
print(f"logs       : {df_logs.shape}")

# ============================================================
# INVESTIGAÇÃO APPLE
# ============================================================

print("\n=== APPLE — Templates crm_cerebro_ads_apple_* ===")
apple = df_camp[df_camp["template"].str.contains("crm_cerebro_ads_apple", na=False)].copy()
apple["date"] = apple["publish_time"].str[:10]

print(apple.groupby(["date", "template", "version"]).size().reset_index(name="count").to_string())

# Check: alguma session da Apple aparece em conversas?
apple_sessions = set(apple["session_id"])
conv_sessions  = set(df_conv["session_id"])
print(f"\nSessions Apple em campanhas : {len(apple_sessions)}")
print(f"Sessions Apple em conversas : {len(apple_sessions & conv_sessions)}")

# Versão registrada vs esperada
print(f"\nVersions registradas : {apple['version'].unique()}")
print(f"Version esperada     : sendtype-835")
print("→ CONCLUSÃO: campo version gravado como '1' — JOIN com sendtype-835 quebra no dashboard")

# ============================================================
# INVESTIGAÇÃO SAMSUNG
# ============================================================

print("\n=== SAMSUNG — template crm_cerebro_galaxys26 ===")
samsung = df_camp[df_camp["template"].str.contains("galaxys26|galaxy_s26", case=False, na=False)]
print(f"Registros em campanhas: {len(samsung)}")

# Clientes interagiram com a campanha?
conv_samsung = df_conv[
    df_conv["text"].str.contains("CUPOMS26|Galaxy S26|Comprar Galaxy", case=False, na=False)
]
print(f"\nConversas com conteúdo Samsung/CUPOMS26: {len(conv_samsung)}")
print(conv_samsung[["session_id", "text", "author", "publish_time"]].head(5).to_string())

# Logs do Omnichannel
MSG = "jsonPayload.message"
samsung_logs = df_logs[
    df_logs[MSG].str.contains("Comprar Galaxy S26|CUPOMS26|cannot be deserialized", case=False, na=False)
]
print(f"\nLogs com erro de deserialização: {len(samsung_logs)}")
print(samsung_logs[MSG].value_counts().head(5))

print("\n→ CONCLUSÃO: CTA 'Comprar Galaxy S26' não foi serializado como JSON")
print("  Pipeline descartou os eventos → tabela campanhas ficou vazia para este template")
print("  Mas clientes RECEBERAM: 4.140 conversas confirmam o envio")

# ============================================================
# RESUMO FINAL
# ============================================================

print("\n" + "=" * 60)
print("RESUMO DA INVESTIGAÇÃO")
print("=" * 60)

resumo = pd.DataFrame([
    {
        "Campanha"       : "Apple (835)",
        "Template"       : "crm_cerebro_ads_apple_1903",
        "Registros"      : len(apple[apple["date"] == "2026-03-19"]),
        "Clientes rcberam": "✅ Sim",
        "No dashboard"   : "❌ Não",
        "Causa Raiz"     : "version='1' ao invés de 'sendtype-835'",
        "Tipo"           : "Modelagem/Ingestão",
    },
    {
        "Campanha"       : "Samsung (838)",
        "Template"       : "crm_cerebro_galaxys26",
        "Registros"      : 0,
        "Clientes rcberam": "✅ Sim (4.140 conversas)",
        "No dashboard"   : "❌ Não",
        "Causa Raiz"     : "CTA não serializado como JSON → pipeline descartou eventos",
        "Tipo"           : "Operacional/Pipeline",
    },
])

print(resumo.to_string(index=False))
