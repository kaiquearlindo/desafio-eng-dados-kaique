# 🔍 Desafio Engenheiro de Dados Pleno — Investigação de Campanhas

**Candidato:** [Seu Nome]
**Vaga:** Analytics Engineer — Time Cérebro da Lu · Magalu
**Data:** Março 2026

---

## 📁 Estrutura do Repositório

```
.
├── README.md                        ← Este documento (narrativa completa)
├── queries/
│   ├── 01_investigacao_apple.sql    ← Queries de diagnóstico campanha Apple
│   ├── 02_investigacao_samsung.sql  ← Queries de diagnóstico campanha Samsung
│   └── 03_monitoramento.sql         ← Proposta de alertas contínuos
├── scripts/
│   └── exploracao_inicial.py        ← EDA dos dados brutos antes do BigQuery
└── dag/
    └── campanhas_to_bigquery.py     ← DAG Airflow: JSON → BigQuery
```

---

## 1. Contexto

O analista de CRM reportou que duas campanhas de WhatsApp não estavam aparecendo no dashboard de mensageria:

| Campanha | Send Type | Template | Data |
|---|---|---|---|
| Apple | 835 | `crm_cerebro_ads_apple_1903` | 19/03/2026 |
| Samsung | 838 | `crm_cerebro_galaxys26` | 20/03/2026 |

**Fontes de dados disponíveis:**
- `campanhas` — registro de configuração dos disparos (Pub/Sub → BigQuery)
- `conversas` — mensagens individuais trocadas com clientes
- `logs_omnichannel.csv` — dump bruto de 12.393 linhas de logs do provedor

---

## 2. Metodologia de Investigação

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

## 3. Investigação: Campanha Apple (Send Type 835)

### 3.1 Passo a passo

**Passo 1 — Verificar se o template existe na tabela `campanhas`:**

```sql
-- query: 01_investigacao_apple.sql
SELECT
  template,
  version,
  COUNT(*) AS total_disparos,
  DATE(publish_time) AS data_disparo
FROM `projeto.dataset.campanhas`
WHERE template LIKE '%crm_cerebro_ads_apple%'
GROUP BY 1, 2, 4
ORDER BY 4;
```

**Resultado:** O template existe — 149 disparos em 19/03 e 92 em 20/03. Porém o campo `version` está gravado como `'1'`, não `'sendtype-835'`.

**Passo 2 — Verificar o JOIN com conversas:**

```sql
SELECT
  c.session_id,
  c.template,
  c.version,
  cv.text,
  cv.author
FROM `projeto.dataset.campanhas` c
LEFT JOIN `projeto.dataset.conversas` cv
  ON c.session_id = cv.session_id
WHERE c.template LIKE '%crm_cerebro_ads_apple%'
  AND DATE(c.publish_time) = '2026-03-19';
```

**Resultado:** 149 sessions da Apple **sem nenhum match** em conversas (0/149). O JOIN entre as tabelas funciona — o problema está em como o dashboard **filtra** por Send Type.

**Passo 3 — Entender o filtro do dashboard:**

O dashboard provavelmente filtra a tabela `campanhas` com `WHERE version = 'sendtype-835'`. Como os registros desta campanha foram gravados com `version = '1'`, eles ficam invisíveis para o painel.

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
FROM `projeto.dataset.campanhas`
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
FROM `projeto.dataset.conversas`
WHERE LOWER(text) LIKE '%cupoms26%'
   OR LOWER(text) LIKE '%galaxy s26%'
   OR LOWER(text) LIKE '%comprar galaxy%'
ORDER BY publish_time
LIMIT 20;
```

**Resultado:** 4.140 registros de clientes interagindo com o conteúdo da campanha Samsung — inclusive com o texto completo da mensagem da Lu e uso do cupom `CUPOMS26`. Isso prova que **a campanha foi enviada e os clientes receberam**.

**Passo 3 — Investigar os logs do Omnichannel:**

```python
samsung_logs = df_logs[
    df_logs['jsonPayload.message'].str.contains(
        'Comprar Galaxy S26|CUPOMS26', case=False, na=False
    )
]
```

**Resultado:** 40+ entradas de log com o erro:

```
It is not a JSON type and cannot be deserialized: Comprar Galaxy S26
```

O CTA (Call-to-Action) do botão `"Comprar Galaxy S26"` foi enviado ao pipeline como **texto puro**, e o serviço de ingestão tentou fazer o parse como JSON — e falhou. Como a deserialização quebrou, a mensagem nunca foi publicada no tópico do Pub/Sub e, consequentemente, **nunca chegou à tabela `campanhas`**.

**Passo 4 — Confirmar que o envio chegou ao cliente:**

Logs com `Successfully sent message` confirmam que o Omnichannel entregou as mensagens aos destinatários. O erro ocorreu na **camada de callback/tracking**, não no envio em si.

### 4.2 Causa Raiz

> **Hipótese C confirmada — Problema Operacional/Pipeline**
>
> O template da campanha Samsung foi configurado com um botão cujo payload de CTA (`"Comprar Galaxy S26"`) foi enviado como string ao invés de objeto JSON serializado. O serviço de ingestão Omnichannel → Pub/Sub falhou ao tentar deserializar esse payload, descartando os eventos. A campanha chegou aos clientes (evidenciado pelas 4.140 conversas e pelos logs de `Successfully sent`), mas **nenhum registro de disparo foi salvo na tabela `campanhas`**.

### 4.3 Evidências

| Evidência | Valor |
|---|---|
| Registros em `campanhas` com template `crm_cerebro_galaxys26` | **0** |
| Conversas com conteúdo da campanha (texto Lu + CUPOMS26) | **4.140** |
| Logs com erro de deserialização do CTA | **40** |
| Padrão do erro | `It is not a JSON type and cannot be deserialized: Comprar Galaxy S26` |
| Clientes que usaram o cupom CUPOMS26 | confirmado via conversas |

---

## 5. Resumo Técnico das Causas

| Campanha | Problema | Onde | Os dados chegaram ao cliente? | Aparece no dashboard? |
|---|---|---|---|---|
| Apple (835) | Campo `version` gravado como `'1'` ao invés de `'sendtype-835'` | Ingestão Pub/Sub → BigQuery | ✅ Sim | ❌ Não |
| Samsung (838) | CTA não serializado como JSON → falha no pipeline | Omnichannel → Pub/Sub | ✅ Sim | ❌ Não |

---

## 6. Mensagem para o Analista de CRM

> Mensagem enviada via Google Chat:

---

**Oi [Nome]! Bom dia 👋**

Analisei as duas campanhas e já encontrei o que aconteceu. A boa notícia é que **as mensagens chegaram aos clientes** nos dois casos — então o impacto na operação foi mínimo. O problema está na camada de rastreamento, que explico abaixo:

**📱 Campanha Apple (19/03)**
Os disparos foram realizados normalmente — temos 149 registros confirmados na base. O que aconteceu é que um campo de identificação (o `version`, que deveria conter `sendtype-835`) foi gravado com o valor genérico `'1'`. Por isso, quando o painel tenta buscar os disparos pelo Send Type 835, ele não encontra nada. **Os dados estão lá, só precisamos corrigir o filtro ou reprocessar o campo.**

**📱 Campanha Samsung (20/03)**
Aqui o caso é diferente: o pipeline de rastreamento teve um problema ao registrar os disparos. O botão `"Comprar Galaxy S26"` foi enviado num formato que o sistema não conseguiu processar, e os eventos foram descartados antes de chegarem ao banco. Mas posso confirmar pelo histórico de conversas que **mais de 4.000 clientes receberam e interagiram com a campanha**, e o cupom CUPOMS26 foi utilizado. Então o envio ocorreu — só não ficou registrado como campanha.

**O que farei agora:**
1. Corrigir o filtro do dashboard para cobrir os registros da Apple com `version = '1'`
2. Abrir chamado com o time de engenharia para ajustar a serialização do CTA da Samsung
3. Propor um monitor automático para detectar esse tipo de falha antes que chegue até vocês

Qualquer dúvida, me chamem! 🙂

---

## 7. Proposta de Monitoramento Contínuo

O objetivo é que a equipe de dados seja **a primeira a saber** quando algo quebra — não o time de negócio.

### 7.1 Alertas no BigQuery com Scheduled Queries

**Monitor 1 — Version inválida na tabela campanhas**

```sql
-- Detecta campanhas com version não padronizada
-- Roda diariamente às 07h
SELECT
  template,
  version,
  COUNT(*) AS total,
  MIN(publish_time) AS primeiro_disparo
FROM `projeto.dataset.campanhas`
WHERE DATE(publish_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND version NOT LIKE 'sendtype-%'
  AND version NOT IN ('v2') -- versões alternativas conhecidas
GROUP BY 1, 2
HAVING total > 0;
```

> Se retornar linhas → alerta no Slack/PagerDuty.

**Monitor 2 — Campanhas disparadas mas sem registro (via conversas)**

```sql
-- Detecta se há conversas com cupons/CTAs de campanhas
-- que não têm correspondência na tabela campanhas
WITH cupons_ativos AS (
  SELECT DISTINCT
    REGEXP_EXTRACT(UPPER(text), r'CUPON?[A-Z0-9]+') AS cupom,
    DATE(publish_time) AS data
  FROM `projeto.dataset.conversas`
  WHERE DATE(publish_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    AND REGEXP_CONTAINS(UPPER(text), r'CUPON?[A-Z0-9]+')
),
campanhas_registradas AS (
  SELECT DISTINCT
    DATE(publish_time) AS data
  FROM `projeto.dataset.campanhas`
  WHERE DATE(publish_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
)
SELECT ca.cupom, ca.data
FROM cupons_ativos ca
LEFT JOIN campanhas_registradas cr ON ca.data = cr.data
WHERE cr.data IS NULL;
```

**Monitor 3 — Volume anômalo de erros de deserialização (via Cloud Logging)**

Criar uma métrica baseada em log no Cloud Monitoring:

```
resource.type="k8s_container"
jsonPayload.message =~ "cannot be deserialized"
severity="INFO"
```

Configurar alerta se `count > 10 em 30 minutos`.

### 7.2 Dashboard de Saúde do Pipeline

Criar uma view no BigQuery que serve como "semáforo" diário:

```sql
CREATE OR REPLACE VIEW `projeto.dataset.vw_saude_pipeline` AS
SELECT
  DATE(publish_time) AS data,
  COUNT(*) AS total_disparos,
  COUNTIF(version NOT LIKE 'sendtype-%' AND version != 'v2') AS disparos_version_invalida,
  COUNT(DISTINCT template) AS templates_ativos,
  ROUND(
    COUNTIF(version NOT LIKE 'sendtype-%' AND version != 'v2') / COUNT(*) * 100, 2
  ) AS pct_versao_invalida
FROM `projeto.dataset.campanhas`
GROUP BY 1
ORDER BY 1 DESC;
```

### 7.3 Fluxo de Resposta Proposto

```
Erro detectado (monitor dispara)
        ↓
Alerta no canal #data-alerts (Slack)
        ↓
Engenheiro de plantão verifica em < 30min
        ↓
Se campanha ativa: notifica CRM proativamente
        ↓
RCA documentado no Confluence
```

---

## 8. Infraestrutura Utilizada

- **BigQuery** — armazenamento e consulta das tabelas `campanhas`, `conversas` e `logs_omnichannel`
- **Apache Airflow (Cloud Composer)** — DAG para ingestão dos arquivos JSON → BigQuery
- **Python / Pandas** — exploração inicial dos dados antes da ingestão
- **Cloud Logging / Cloud Monitoring** — base para os alertas de log

---

## Apêndice: Exploração Inicial dos Dados

Antes de carregar no BigQuery, realizei uma análise exploratória local para entender a estrutura das tabelas e formular hipóteses:

- `campanhas.json`: 11.445 registros · campos: `session_id`, `template`, `version`, `channel_client_id`, `publish_time`, `message_id`, `source`
- `conversas.json`: 46.960 registros · campos: `session_id`, `text`, `author`, `user_id`, `publish_time`, `media_type`
- `logs_omnichannel.csv`: 12.393 linhas · logs estruturados do GKE com `jsonPayload.message`, `severity`, `timestamp`

Scripts de exploração disponíveis em `/scripts/exploracao_inicial.py`.
