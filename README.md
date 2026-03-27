# 🔍 Desafio Engenheiro de Dados Pleno — Investigação de Campanhas

**Candidato:** Kaique Arlindo
**Vaga:** Analytics Engineer — Time Cérebro da Lu 
**Data:** Março 2026

---

## 📁 Estrutura do Repositório

```
.
├── README.md                                    ← Este documento (narrativa completa)
├── 01_investigacao_apple.sql                    ← Queries de diagnóstico campanha Apple
├── 02_investigacao_samsung.sql                  ← Queries de diagnóstico campanha Samsung
├── exploracao_inicial.py                        ← First Análise dos dados brutos: registros e campos de cada fonte
└── cerebro_lu_logs_load/                        ← Pipeline de ingestão (DAG/Sness)
    ├── README.md                                ← Descrição do projeto de ingestão
    ├── dags/
    │   └── main.py                              ← DAG Airflow: orquestração da carga JSON → BigQuery
    └── etls/
        ├── raw/
        │   ├── ingest_campanhas.py              ← Job PySpark: campanhas.json → temp_bq.campanhas
        │   └── ingest_conversas.py              ← Job PySpark: conversas.json → temp_bq.conversas
        ├── trusted/                             
        ├── refined/                             
        └── ml/                                  
```

---

## 1. Contexto

O analista de CRM reportou que duas campanhas de WhatsApp não estavam aparecendo no dashboard de mensageria:

| Campanha | Send Type | Template | Data |
|---|---|---|---|
| Apple | 835 | `crm_cerebro_ads_apple_1903` | 19/03/2026 |
| Samsung | 838 | `crm_cerebro_galaxys26` | 20/03/2026 |

**Fontes de dados disponíveis:**
- `campanhas` — registro de configuração dos disparos 
- `conversas` — mensagens individuais trocadas com clientes
- `logs_omnichannel.csv` — dump bruto de 12.393 linhas de logs do provedor

---

## 2 .Exploração Inicial dos Dados

Antes de pensar em carregar no BigQuery, realizei uma análise exploratória local para entender a estrutura das tabelas:

**campanhas.json**: 11.445 registros · campos: `session_id`, `template`, `version`, `channel_client_id`, `publish_time`, `message_id`, `source`

**conversas.json**: 46.960 registros · campos: `session_id`, `text`, `author`, `user_id`, `publish_time`, `media_type`

**logs_omnichannel.csv**: 12.393 linhas · logs estruturados do GKE com `jsonPayload.message`, `severity`, `timestamp`


## 2.1 Metodologia de Investigação

Antes de olhar os dados, formulei as hipóteses em três categorias:

```
Hipótese A → Problema no Dashboard    (JOIN errado, filtro de data, campo nulo)
Hipótese B → Problema na Modelagem    (campo gravado errado, tipo inconsistente)
Hipótese C → Problema Operacional     (disparo não aconteceu, erro no pipeline)
```

O fluxo de investigação seguiu a pirâmide: fonte bruta → tabela de staging → dashboard.

```
[Logs Omnichannel]  →  [campanhas]  →  [conversas]  →  [Dashboard]
      ↓ dado bruto        ↓ staging        ↓ join            ↓ viz
```

Se o dado existe na fonte bruta mas não no dashboard, o problema está no meio do caminho.

---
## 3. Preparação dos Dados para Análise e Investigação no BigQuery
Antes de iniciar as consultas de investigação, pensei em disponibilizar as três fontes de dados no BigQuery, caso a **área de CRM ou alguma outra equipe** queira tentar entender melhor o processo de **troubleshooting** feito neste caso. 

Sendo assim, segui uma estratégia diferente para cada fonte, de acordo com seu formato de origem.

**3.1 Tabelas campanhas e conversas — Ingestão via DAG Airflow + PySpark**
Os arquivos `campanhas.json` e `conversas.json` foram ingeridos no BigQuery através de uma DAG que desenvolvi - Airflow (`CerebroLuLogsLoad`), utilizando o padrão interno da Plataforma com `MinecraftOperator` e jobs PySpark no Dataproc.

O fluxo de cada job segue as etapas:

```campanhas.json / conversas.json (GCS)
        ↓
  PySpark (Dataproc)
  - Leitura multiLine JSON
  - Cast de attributes → STRING
  - Parse de publish_time → TIMESTAMP
        ↓
  BigQuery
  maga-bigdata.temp_bq.campanhas
  maga-bigdata.temp_bq.conversas
```

**3.2 Tabela logs_omnichannel — Tabela Externa via Google Sheets**
O arquivo `logs_omnichannel.csv` por ser um csv, decidi seguir uma abordagem diferente e mais rápida, realizei a importação dele para Planilha do Google e conectado diretamente ao BigQuery como tabela externa (`GOOGLE_SHEETS`), sem necessidade de pipeline de ingestão.

```logs_omnichannel.csv
        ↓
  Google Sheets
  (detecção automática de schema)
        ↓
  BigQuery — Tabela Externa
  maga-bigdata.temp_bq.logs_omnichannel
```
Essa abordagem mantém o dado acessível via SQL junto às demais tabelas, e permite cruzar os logs com campanhas e conversas diretamente no BigQuery.


## 4. Investigação: Campanha Apple (Send Type 835)

### 4.1 Passo a passo

**Passo 1 — Verificar se o template existe na tabela `campanhas`:**

```SELECT
  template,
  version,
  COUNT(*)           AS total_disparos,
  DATE(publish_time) AS data_disparo
FROM `temp_bq.campanhas`
WHERE template IN ('crm_cerebro_ads_apple_1903', 'crm_cerebro_galaxys26')
   OR template LIKE '%crm_cerebro_ads_apple%'
GROUP BY 1, 2, 4
ORDER BY 4;
```

**Resultado:** O template existe — 149 disparos em 19/03 e 92 em 20/03. Porém o campo `version` está gravado como `'1'`, não `'sendtype-835'`.

**Passo 2 — Verificar todos os send types (versions) que existem na tabela campanhas:**

```sql
SELECT
  version,
  COUNT(*) AS total
FROM `temp_bq.campanhas`
GROUP BY 1
ORDER BY 2 DESC;
```

**Resultado:** Não localizado templates 'sendtype-835'.


**Passo 3 — Verificar o JOIN com conversas:**

```SELECT
  c.session_id,
  c.template,
  c.version,
  cv.text,
  cv.author
FROM `temp_bq.campanhas` c
LEFT JOIN `temp_bq.conversas` cv
  ON c.session_id = cv.session_id
WHERE c.template LIKE '%crm_cerebro_ads_apple%'
  AND DATE(c.publish_time) = '2026-03-19';
```
**Resultado:** 149 sessions da Apple sem nenhum match em conversas (0/149). 
O JOIN entre as tabelas funciona — o problema está em como o dashboard filtra por Send Type.


**Passo 4 — Confirmar que o dashboard filtra por sendtype e está quebrando
-- Simula o filtro que provavelmente está no dashboard**

```SELECT COUNT(*) AS resultado_com_filtro_dashboard
FROM `temp_bq.campanhas`
WHERE version = 'sendtype-835'
  AND DATE(publish_time) = '2026-03-19';
```
**Resultado:** 0 registros - CONFIRMA: o dashboard não acha os dados porque version = '1', não 'sendtype-835'
O dashboard provavelmente filtra a tabela `campanhas` com `WHERE version = 'sendtype-835'`. 

**Passo 5 — Confirmar a inconsistência comparando com outras campanhas Apple
-- que têm o campo version preenchido corretamente**
```SELECT
  template,
  version,
  COUNT(*) AS total,
  DATE(publish_time) AS data
FROM `temp_bq.campanhas`
WHERE template LIKE '%apple%'
GROUP BY 1, 2, 4
ORDER BY 4;
```

Resultado:
PADRÃO NORMAL (campanhas Natal):
  apple7_natal_2025    | sendtype-758 | 11 | 2026-...
  apple16e_natal_2025  | sendtype-757 | 5  | 2026-...

PADRÃO QUEBRADO (campanha Cérebro):
  crm_cerebro_ads_apple_* | 1 | N | 2026-03-19


### 3.2 Causa Raiz

> **Hipótese B confirmada — Problema de Modelagem/Ingestão**
>
> O campo `version` dos registros da campanha Apple foi gravado como `'1'` em vez de `'sendtype-835'`. Trata-se de um erro na parametrização do Pub/Sub no momento da publicação da campanha. Os dados existem na tabela, mas o dashboard não os encontra porque filtra pelo valor esperado (`sendtype-835`).

### 3.3 Evidências

| Evidência | Valor |
|---|---|
| Disparos registrados em `campanhas` | 241 (149 em 19/03 + 92 em 20/03) |
| Version registrada | `'1'` |
| Version esperada (Send Type) | `'sendtype-835'` |
| Sessions com match em `conversas` | 0 de 144 |
| Templates semelhantes com version correta | `sendtype-757`, `sendtype-758` (campanhas Apple Natal) |

---

## 4. Investigação: Campanha Samsung Galaxy S26 (Send Type 838)

### 4.1 Passo a passo

**Passo 1 — Verificar se o template existe na tabela `campanhas`:**

```sql
SELECT COUNT(*)
FROM `temp_bq.campanhas`
WHERE template = 'crm_cerebro_galaxys26';
```

**Resultado: 0 registros.** A campanha não existe na tabela de campanhas.

**Passo 2 — Verificar se chegou alguma resposta de cliente em `conversas`:**

```sql
SELECT
  session_id,
  text,
  author,
  publish_time
FROM `temp_bq.conversas`
WHERE LOWER(text) LIKE '%cupoms26%'
   OR LOWER(text) LIKE '%galaxy s26%'
   OR LOWER(text) LIKE '%comprar galaxy%'
ORDER BY publish_time
```

**Resultado:** 4.140 registros de clientes interagindo com o conteúdo da campanha Samsung — inclusive com o texto completo da mensagem da Lu e uso do cupom `CUPOMS26`. Isso prova que **a campanha foi enviada e os clientes receberam**.

**Passo 3 — Investigar os logs do Omnichannel:**

```SELECT
  jsonPayload_message    AS mensagem,
  severity,
  timestamp,
  COUNT(*)               AS ocorrencias
FROM `temp_bq.logs_omnichannel`
WHERE LOWER(jsonPayload_message) LIKE '%cannot be deserialized%'
   OR LOWER(jsonPayload_message) LIKE '%comprar galaxy s26%'
   OR LOWER(jsonPayload_message) LIKE '%cupoms26%'
GROUP BY 1, 2, 3
ORDER BY 3;
```

**Resultado:** Muitas entradas de log com o erro: `It is not a JSON type and cannot be deserialized:` + mensagem.

`
O CTA (Call-to-Action) do botão `"Comprar Galaxy S26"` foi enviado ao pipeline como **texto puro**, e o serviço de ingestão tentou fazer o parse como JSON — e falhou. Como a deserialização quebrou, a mensagem nunca foi publicada no tópico do Pub/Sub e, consequentemente, **nunca chegou à tabela `campanhas`**.

**Passo 4 — Confirmar que o envio chegou ao cliente:**

```SELECT
  CASE
    WHEN LOWER(jsonPayload_message) LIKE '%successfully sent%'    THEN 'Entregue ao cliente'
    WHEN LOWER(jsonPayload_message) LIKE '%cannot be deserialized%' THEN 'Erro de deserialização'
    ELSE 'Outro'
  END                  AS tipo_evento,
  COUNT(*)             AS ocorrencias
FROM `temp_bq.logs_omnichannel`
WHERE LOWER(jsonPayload_message) LIKE '%successfully sent%'
   OR LOWER(jsonPayload_message) LIKE '%cannot be deserialized%'
GROUP BY 1
ORDER BY 2 DESC;
```

**Resultado:** Logs com Successfully sent message confirmam que o Omnichannel entregou as mensagens aos destinatários. O erro ocorreu na camada de callback/tracking, não no envio em si.

### 4.2 Causa Raiz

> **Hipótese C confirmada — Problema Operacional/Pipeline**
>
> O template da campanha Samsung foi configurado com um botão cujo payload de CTA (`"Comprar Galaxy S26"`) foi enviado como string ao invés de objeto JSON serializado. O serviço de ingestão Omnichannel → Pub/Sub falhou ao tentar deserializar esse payload, descartando os eventos. A campanha chegou aos clientes (evidenciado pelas 4.140 conversas e pelos logs de `Successfully sent`), mas **nenhum registro de disparo foi salvo na tabela `campanhas`**.

### 4.3 Evidências

| Evidência | Valor |
|---|---|
| Registros em `campanhas` com template `crm_cerebro_galaxys26` | **0** |
| Conversas com conteúdo da campanha (texto Lu + CUPOMS26) | **4.140** |
| Logs com erro de deserialização do CTA | **+1000** |
| Padrão do erro | `It is not a JSON type and cannot be deserialized: Comprar Galaxy S26` |
| Clientes que usaram o cupom CUPOMS26 | confirmado via conversas |

---

## 5. Resumo Técnico das Causas

| Campanha | Problema | Onde | Os dados chegaram ao cliente? | Aparece no dashboard? |
|---|---|---|---|---|
| Apple (835) | Campo `version` gravado como `'1'` ao invés de `'sendtype-835'` | Ingestão Pub/Sub → Fonte | ✅ Sim | ❌ Não |
| Samsung (838) | CTA não serializado como JSON → falha no pipeline | Omnichannel → Pub/Sub | ✅ Sim | ❌ Não |

---

## 6. Mensagem para o Analista de CRM

---

Oi [Analista de CRM]. Boa tarde, tudo bem?

Analisamos aqui as duas campanhas e conseguimos entender o que aconteceu. A boa notícia é que as mensagens chegaram aos clientes nos dois casos, então o impacto na operação foi mínimo. O problema em si está na camada de rastreamento, que explicarei melhor abaixo:

1. Campanha Apple (19/03): Os disparos foram realizados normalmente — temos 149 registros confirmados na base. O que aconteceu é que um campo de identificação (o version, que deveria conter sendtype-835) foi gravado com o valor genérico '1'. Por isso, quando o dashboard tenta buscar os disparos pelo sendtype-835, ele não encontra nada. 

Os dados estão lá, para resolver inicialmente precisamos corrigir no dashboard o filtro: 

Trocar o filtro da versão que está pegando somente um template, e incluir também o template com valor '1', posso auxiliar nesta mudança e validarmos juntos.

version = 'sendtype-835' por version IN ('sendtype-835', '1') AND template LIKE '%crm_cerebro_ads_apple% .

2. Campanha Samsung (20/03): Aqui o caso é diferente: o fluxo de rastreamento teve um problema ao registrar os disparos. O botão "Comprar Galaxy S26" foi enviado num formato que o sistema não conseguiu processar, e os eventos foram descartados antes de chegarem ao banco. Mas posso confirmar pelo histórico de conversas que mais de 4.000 clientes receberam e interagiram com a campanha, e o cupom CUPOMS26 foi utilizado. Então o envio ocorreu, só não ficou registrado como campanha na base de dados.

Sendo assim, irei trocar internamente com o time aqui estes pontos, para realizarmos a correção e nas próximas conseguirmos detectar essas anomalias antes que cheguem até vocês.

Qualquer dúvida, estamos à disposição.

---

## 8. Próximos Passos

1. Verificar a possibilidade de corrigir na origem e investigar por que o Pub/Sub gravou '1' ao invés de 'sendtype-835', para os próximos disparos já chegarem corretos para nós na raw.

2. Verificar com a equipe para ajustar a serialização do CTA da Samsung.

3. Propor um monitorias automáticas para detectar esse tipo de falha antes que cheguem a outras equipes.

O objetivo é que a equipe de dados seja **a primeira a saber** quando algo quebra — não o time de negócio.

## 8.1 Proposta de Monitoramento Contínuo

O objetivo é que a equipe de dados seja a primeira a saber quando algo quebra — não o time de negócio.

Para evitar que o time de CRM descubra o erro antes de nós, proponho:

Monitoramento de Logs (Log-based Metrics): Criar um alerta no Cloud Monitoring (GCP) ou DAG, que dispara uma notificação em um canal da equipe no GChat, sempre que o erro "cannot be deserialized" ocorrer mais de 10 vezes em 5 minutos. Isso nos permite agir no exato momento da falha do envio.

Data Quality Check (Integrity Check): Implementar um teste de integridade (via dbt ou Airflow) que compare diariamente a lista de templates ativos no Log do Provedor com a lista de templates na tabela campanhas. Se houver um template com volume de disparos > 0 que não existe no cadastro, um relatório de "Campanhas Não Mapeadas" é gerado automaticamente.

Validação de Schema no Ingestor: Ajustar o serviço de mensageria para rejeitar payloads mal-formatados já na origem, enviando um log de erro mais descritivo que aponte exatamente qual campo do JSON está inválido.


## 9. Infraestrutura Utilizada

BigQuery — armazenamento e consulta das tabelas campanhas, conversas e logs_omnichannel

Projeto: maga-bigdata Dataset: temp_bq

Origem dos arquivos: gs://stg-lake-raw-data_governance/

Apache Airflow — orquestração via DAG CerebroLuLogsLoad usando o padrão interno do Magalu

MinecraftOperator para submissão dos jobs PySpark no Dataproc

build_ness_etl_path para resolução dos caminhos dos scripts (sness)

Duas tasks de ingestão (ingest_campanhas e ingest_conversas) rodando em paralelo

PySpark — jobs de leitura e escrita para o BigQuery

ingest_campanhas.py: leitura multiLine JSON, cast de attributes para STRING, parse de publish_time para TIMESTAMP

ingest_conversas.py: leitura multiLine JSON, parse de publish_time para TIMESTAMP

Escrita com mode("overwrite") via conector BigQuery com bucket temporário GCS

Python / Pandas — exploração inicial dos dados antes da ingestão

-------
