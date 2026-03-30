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
├── exploracao_inicial.py                        ← Análise inicial dos dados brutos: registros e campos de cada fonte
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
- `logs_omnichannel.csv` — dump bruto de logs do provedor de mensageria

---

## 2. Exploração Inicial dos Dados

Antes de pensar em carregar no BigQuery, realizei uma análise exploratória local para entender a estrutura das tabelas:

- **`campanhas.json`**: 11.445 registros · campos: `session_id`, `template`, `version`, `channel_client_id`, `publish_time`, `message_id`, `source`
- **`conversas.json`**: 46.960 registros · campos: `session_id`, `text`, `author`, `user_id`, `publish_time`, `media_type`
- **`logs_omnichannel.csv`**: 12.393 linhas · logs estruturados do GKE com `jsonPayload.message`, `severity`, `timestamp` (no cluster retorna 10.000 linhas devido limite)

Script disponível em: `exploracao_inicial.py`

### 2.1 Metodologia de Investigação

Antes de olhar os dados, formulei as hipóteses em três categorias:

```
Hipótese A → Problema no Dashboard    (JOIN errado, filtro de data, campo nulo)
Hipótese B → Problema na Modelagem    (campo gravado errado, tipo inconsistente)
Hipótese C → Problema Operacional     (disparo não aconteceu, erro no pipeline)
```

O fluxo de investigação seguiu a pirâmide: fonte bruta → staging → dashboard.

```
[Logs Omnichannel]  →  [campanhas]  →  [conversas]  →  [Dashboard]
      ↓ dado bruto        ↓ staging        ↓ join            ↓ viz
```

Se o dado existe na fonte bruta mas não no dashboard, o problema está no meio do caminho.

---

## 3. Preparação dos Dados para Análise no BigQuery

Antes de iniciar as consultas de investigação, disponibilizei as três fontes de dados no BigQuery — caso a área de CRM ou alguma outra equipe queira acompanhar o processo de troubleshooting. Cada fonte seguiu uma estratégia diferente, de acordo com seu formato de origem.

### 3.1 Tabelas `campanhas` e `conversas` — Ingestão via DAG Airflow + PySpark

Os arquivos `campanhas.json` e `conversas.json` foram ingeridos no BigQuery através de uma DAG Airflow (`CerebroLuLogsLoad`), utilizando o padrão interno da plataforma com `MinecraftOperator` e jobs PySpark no Dataproc.

O fluxo de cada job:

```
campanhas.json / conversas.json (GCS)
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

As duas tasks de ingestão rodam em **paralelo** após o operador `Start`, com alertas automáticos de falha via `slack_failed_task`.

### 3.2 Tabela `logs_omnichannel` — Tabela Externa via Google Sheets

O arquivo `logs_omnichannel.csv` seguiu uma abordagem diferente e mais rápida: foi importado para uma Planilha do Google e conectado diretamente ao BigQuery como **tabela externa** (`GOOGLE_SHEETS`), sem necessidade de pipeline de ingestão.

```
logs_omnichannel.csv
        ↓
  Google Sheets
  (detecção automática de schema)
        ↓
  BigQuery — Tabela Externa
  maga-bigdata.temp_bq.logs_omnichannel
```

Essa abordagem mantém o dado acessível via SQL junto às demais tabelas, permitindo cruzar os logs com `campanhas` e `conversas` diretamente no BigQuery.

### 3.3 Visão Consolidada das Fontes no BigQuery

| Tabela | Origem | Estratégia | Registros |
|---|---|---|---|
| `temp_bq.campanhas` | `campanhas.json` (GCS) | DAG Airflow + PySpark (Dataproc) | 11.445 |
| `temp_bq.conversas` | `conversas.json` (GCS) | DAG Airflow + PySpark (Dataproc) | 46.960 |
| `temp_bq.logs_omnichannel` | `logs_omnichannel.csv` | Google Sheets → Tabela Externa BQ | 10.000 |

---

## 4. Investigação: Campanha Apple (Send Type 835)

### 4.1 Passo a Passo

**Passo 1 — Verificar se o template existe na tabela `campanhas`:**

```sql
SELECT
  template,
  version,
  COUNT(*)           AS total_disparos,
  DATE(publish_time) AS data_disparo
FROM `maga-bigdata.temp_bq.campanhas`
WHERE template IN ('crm_cerebro_ads_apple_1903', 'crm_cerebro_galaxys26')
   OR template LIKE '%crm_cerebro_ads_apple%'
GROUP BY 1, 2, 4
ORDER BY 4;
```

**Resultado:** O template existe — 149 disparos em 19/03 e 92 em 20/03. Porém o campo `version` está gravado como `'1'`, não `'sendtype-835'`.

---

**Passo 2 — Verificar todos os send types existentes na tabela:**

```sql
SELECT
  version,
  COUNT(*) AS total
FROM `maga-bigdata.temp_bq.campanhas`
GROUP BY 1
ORDER BY 2 DESC;
```

**Resultado:** `sendtype-835` não foi localizado em nenhum registro.

---

**Passo 3 — Verificar o JOIN com conversas:**

```sql
SELECT
  c.session_id,
  c.template,
  c.version,
  cv.text,
  cv.author
FROM `maga-bigdata.temp_bq.campanhas` c
LEFT JOIN `maga-bigdata.temp_bq.conversas` cv
  ON c.session_id = cv.session_id
WHERE c.template LIKE '%crm_cerebro_ads_apple%'
  AND DATE(c.publish_time) = '2026-03-19';
```

**Resultado:** 149 sessions da Apple sem nenhum match em conversas (0/149). O JOIN funciona — o problema está em como o dashboard filtra por Send Type.

---

**Passo 4 — Simular o filtro do dashboard:**

```sql
SELECT COUNT(*) AS resultado_com_filtro_dashboard
FROM `maga-bigdata.temp_bq.campanhas`
WHERE version = 'sendtype-835'
  AND DATE(publish_time) = '2026-03-19';
```

**Resultado:** 0 registros. Confirmado: o dashboard não encontra os dados porque `version = '1'`, não `'sendtype-835'`.

---

**Passo 5 — Comparar com campanhas Apple que têm `version` correta:**

```sql
SELECT
  template,
  version,
  COUNT(*)           AS total,
  DATE(publish_time) AS data
FROM `maga-bigdata.temp_bq.campanhas`
WHERE template LIKE '%apple%'
GROUP BY 1, 2, 4
ORDER BY 4;
```

**Resultado:**

| template | version | total | data |
|---|---|---|---|
| `apple7_natal_2025` | `sendtype-758` | 11 | 2026-... |
| `apple16e_natal_2025` | `sendtype-757` | 5 | 2026-... |
| `crm_cerebro_ads_apple_*` | `1` | N | 2026-03-19 |

Campanhas Apple anteriores seguem o padrão `sendtype-XXX`. A campanha do Cérebro da Lu foi a única gravada com `'1'`.

### 4.2 Causa Raiz

> **Hipótese B confirmada — Problema de Modelagem/Ingestão**
>
> O campo `version` dos registros da campanha Apple foi gravado como `'1'` em vez de `'sendtype-835'`. Trata-se de um erro na parametrização do Pub/Sub no momento da publicação da campanha. Os dados existem na tabela, mas o dashboard não os encontra porque filtra pelo valor esperado (`sendtype-835`).

### 4.3 Evidências

| Evidência | Valor |
|---|---|
| Disparos registrados em `campanhas` | 241 (149 em 19/03 + 92 em 20/03) |
| Version registrada | `'1'` |
| Version esperada (Send Type) | `'sendtype-835'` |
| Sessions com match em `conversas` | 0 de 149 |
| Templates semelhantes com version correta | `sendtype-757`, `sendtype-758` (campanhas Apple Natal) |

---

## 5. Investigação: Campanha Samsung Galaxy S26 (Send Type 838)

### 5.1 Passo a Passo

**Passo 1 — Verificar se o template existe na tabela `campanhas`:**

```sql
SELECT COUNT(*) AS registros_samsung
FROM `maga-bigdata.temp_bq.campanhas`
WHERE template = 'crm_cerebro_galaxys26'
   OR version  = 'sendtype-838';
```

**Resultado:** 0 registros. A campanha não existe na tabela de campanhas.

---

**Passo 2 — Verificar se os clientes receberam via `conversas`:**

```sql
SELECT
  session_id,
  text,
  author,
  publish_time
FROM `maga-bigdata.temp_bq.conversas`
WHERE LOWER(text) LIKE '%cupoms26%'
   OR LOWER(text) LIKE '%galaxy s26%'
   OR LOWER(text) LIKE '%comprar galaxy%'
ORDER BY publish_time;
```

**Resultado:** 4.140 registros de clientes interagindo com o conteúdo da campanha — inclusive com o texto completo da mensagem da Lu e uso do cupom `CUPOMS26`. Isso prova que **a campanha foi enviada e os clientes receberam**.

---

**Passo 3 — Investigar os logs do Omnichannel:**

```sql
SELECT
  jsonPayload_message AS mensagem,
  severity,
  DATE(timestamp)     AS data,
  COUNT(*)            AS ocorrencias
FROM `maga-bigdata.temp_bq.logs_omnichannel`
WHERE LOWER(jsonPayload_message) LIKE '%cannot be deserialized%'
   OR LOWER(jsonPayload_message) LIKE '%comprar galaxy s26%'
   OR LOWER(jsonPayload_message) LIKE '%cupoms26%'
GROUP BY 1, 2, 3
ORDER BY 4 DESC;
```

**Resultado:** Muitas entradas de log com o erro:

```
It is not a JSON type and cannot be deserialized: Comprar Galaxy S26
```

O CTA `"Comprar Galaxy S26"` foi enviado ao pipeline como **texto puro**. O serviço de ingestão tentou fazer o parse como JSON — e falhou. Como a deserialização quebrou, o evento nunca foi publicado no tópico do Pub/Sub e, consequentemente, **nunca chegou à tabela `campanhas`**.

---

**Passo 4 — Confirmar que o envio chegou ao cliente:**

```sql
SELECT
  CASE
    WHEN LOWER(jsonPayload_message) LIKE '%successfully sent%'      THEN 'Entregue ao cliente'
    WHEN LOWER(jsonPayload_message) LIKE '%cannot be deserialized%' THEN 'Erro de deserialização'
    ELSE 'Outro'
  END          AS tipo_evento,
  COUNT(*)     AS ocorrencias
FROM `maga-bigdata.temp_bq.logs_omnichannel`
WHERE LOWER(jsonPayload_message) LIKE '%successfully sent%'
   OR LOWER(jsonPayload_message) LIKE '%cannot be deserialized%'
GROUP BY 1
ORDER BY 2 DESC;
```

**Resultado:** Logs com `Successfully sent message` confirmam que o Omnichannel entregou as mensagens aos destinatários. O erro ocorreu na **camada de callback/tracking**, não no envio em si.

### 5.2 Causa Raiz

> **Hipótese C confirmada — Problema Operacional/Pipeline**
>
> O template da campanha Samsung foi configurado com um botão cujo payload de CTA (`"Comprar Galaxy S26"`) foi enviado como string ao invés de objeto JSON serializado. O serviço de ingestão Omnichannel → Pub/Sub falhou ao tentar deserializar esse payload, descartando os eventos. A campanha chegou aos clientes — evidenciado pelas 4.140 conversas e pelos logs de `Successfully sent` — mas **nenhum registro de disparo foi salvo na tabela `campanhas`**.

### 5.3 Evidências

| Evidência | Valor |
|---|---|
| Registros em `campanhas` com template `crm_cerebro_galaxys26` | **0** |
| Conversas com conteúdo da campanha (texto Lu + CUPOMS26) | **4.140** |
| Logs com erro de deserialização do CTA | **+1.000** |
| Padrão do erro | `It is not a JSON type and cannot be deserialized: Comprar Galaxy S26` |
| Uso do cupom CUPOMS26 | Confirmado via conversas |

---

## 6. Resumo Técnico das Causas

| Campanha | Problema | Onde | Clientes receberam? | Aparece no dashboard? |
|---|---|---|---|---|
| Apple (835) | Campo `version` gravado como `'1'` ao invés de `'sendtype-835'` | Ingestão Pub/Sub → BigQuery | ✅ Sim | ❌ Não |
| Samsung (838) | CTA não serializado como JSON → falha no pipeline | Omnichannel → Pub/Sub | ✅ Sim | ❌ Não |

---

## 7. Mensagem para o Analista de CRM

> Mensagem enviada via Google Chat:

---

Oi [Analista de CRM], boa tarde! Tudo bem?

Analisamos as duas campanhas e conseguimos entender o que aconteceu. A boa notícia é que **as mensagens chegaram aos clientes** nos dois casos — então o impacto na operação foi mínimo. O problema está na camada de rastreamento, que explico abaixo:

**📱 Campanha Apple (19/03)**
Os disparos foram realizados normalmente — temos 149 registros confirmados na base. O que aconteceu é que um campo de identificação (o `version`, que deveria conter `sendtype-835`) foi gravado com o valor genérico `'1'`. Por isso, quando o dashboard tenta buscar os disparos pelo Send Type 835, ele não encontra nada.

Os dados estão lá. Para resolver no curto prazo, precisamos ajustar o filtro do dashboard — trocar:

```
version = 'sendtype-835'
```
por:
```
version IN ('sendtype-835', '1') AND template LIKE '%crm_cerebro_ads_apple%'
```

Posso auxiliar nessa mudança e validarmos juntos.

**📱 Campanha Samsung (20/03)**
Aqui o caso é diferente: o fluxo de rastreamento teve um problema ao registrar os disparos. O botão `"Comprar Galaxy S26"` foi enviado num formato que o sistema não conseguiu processar, e os eventos foram descartados antes de chegarem ao banco. Mas posso confirmar pelo histórico de conversas que **mais de 4.000 clientes receberam e interagiram com a campanha**, e o cupom CUPOMS26 foi utilizado. Então o envio ocorreu — só não ficou registrado como campanha na base de dados.

Vou tratar internamente com o time os pontos técnicos de cada caso para realizarmos as correções. Nas próximas campanhas, teremos monitoramento automático para detectar essas anomalias antes que cheguem até vocês.

Qualquer dúvida, estamos à disposição! 🙂

---

## 8. Próximos Passos e Monitoramento

### 8.1 Correções Imediatas

1. **Apple** — Investigar por que o Pub/Sub gravou `version = '1'` ao invés de `'sendtype-835'` e corrigir na origem, para que os próximos disparos já cheguem corretos na camada raw. No curto prazo, ajustar o filtro do dashboard conforme descrito na mensagem acima.

2. **Samsung** — Alinhar com a equipe de engenharia para ajustar a serialização do CTA, garantindo que o payload do botão chegue ao pipeline como objeto JSON e não como string pura.

### 8.2 Proposta de Monitoramento Contínuo

O objetivo é que a equipe de dados seja **a primeira a saber** quando algo quebra — não o time de negócio.

**Monitoramento de Logs (Log-based Metrics)**
Criar um alerta no Cloud Monitoring (GCP) que dispara uma notificação no canal da equipe no Google Chat sempre que o erro `"cannot be deserialized"` ocorrer mais de 10 vezes em 5 minutos. Isso permite agir no exato momento da falha, antes que qualquer campanha passe despercebida.

**Data Quality Check (Integrity Check)**
Implementar um teste de integridade via dbt ou Airflow que rode diariamente e faça duas verificações:

**Verificação A — Campanhas Não Mapeadas (via conversas):**
Comparar os `session_id` presentes em `conversas` com os `session_id` presentes em `campanhas`. Se existirem sessões em conversas cujo conteúdo contenha padrões de campanha (cupons, CTAs conhecidos) mas sem correspondência em `campanhas`, um relatório de "Campanha Disparada Sem Registro" é gerado automaticamente.

**Verificação B — Templates com Version Inválida:** Checar diariamente se existem registros na tabela `campanhas` com `version` fora do padrão `sendtype-XXX`. Esse check teria detectado o problema da Apple no dia seguinte ao disparo.

**Validação de Schema no Ingestor**
Ajustar o serviço de mensageria para rejeitar payloads mal-formatados já na origem, enviando um log de erro descritivo que aponte exatamente qual campo do JSON está inválido — facilitando o diagnóstico rápido em casos futuros.

---

## 9. Infraestrutura Utilizada

**BigQuery** — armazenamento e consulta das tabelas `campanhas`, `conversas` e `logs_omnichannel`
- Projeto: `maga-bigdata` · Dataset: `temp_bq`

**Apache Airflow** — orquestração via DAG `CerebroLuLogsLoad` usando o padrão interno do Magalu
- `MinecraftOperator` para submissão dos jobs PySpark no Dataproc
- `build_ness_etl_path` para resolução dos caminhos dos scripts (`sness`)
- Tasks `ingest_campanhas` e `ingest_conversas` rodando em paralelo
- Alertas automáticos de falha via `slack_failed_task`

**PySpark** — jobs de leitura e escrita para o BigQuery
- `ingest_campanhas.py`: leitura multiLine JSON, cast de `attributes` para STRING, parse de `publish_time` para TIMESTAMP
- `ingest_conversas.py`: leitura multiLine JSON, parse de `publish_time` para TIMESTAMP
- Escrita com `mode("overwrite")` via conector BigQuery com bucket temporário GCS

**Cluster de Exploração (Jupyter/Python/Pandas)** — exploração inicial dos dados antes da ingestão ao BQ e análises investigativas. 

**Gemini** — utilizado como assistente durante o processo de investigação para apoio, estruturação e formatação da narrativa do README e Documentação. 
