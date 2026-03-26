-- ============================================================
-- 02_investigacao_samsung.sql
-- Investigação: Campanha Samsung Galaxy S26 (Send Type 838 / crm_cerebro_galaxys26)
-- ============================================================

-- PASSO 1: Verificar se o template existe na tabela campanhas
SELECT
  template,
  version,
  COUNT(*)           AS total_disparos,
  DATE(publish_time) AS data_disparo
FROM `projeto.dataset.campanhas`
WHERE template = 'crm_cerebro_galaxys26'
GROUP BY 1, 2, 4;

/*
RESULTADO: 0 registros
→ A campanha simplesmente não existe na tabela campanhas
*/


-- PASSO 2: Buscar por qualquer variação do nome
SELECT
  template,
  version,
  COUNT(*) AS total,
  DATE(publish_time) AS data
FROM `projeto.dataset.campanhas`
WHERE LOWER(template) LIKE '%galaxy%'
   OR LOWER(template) LIKE '%samsung%'
   OR LOWER(template) LIKE '%s26%'
GROUP BY 1, 2, 4;

/*
RESULTADO: 0 registros
→ Nenhuma variação do template Samsung existe na tabela
*/


-- PASSO 3: Verificar se os clientes receberam e responderam (prova de envio)
SELECT
  session_id,
  text,
  author,
  publish_time,
  media_type
FROM `projeto.dataset.conversas`
WHERE LOWER(text) LIKE '%cupoms26%'
   OR LOWER(text) LIKE '%galaxy s26%'
   OR LOWER(text) LIKE '%comprar galaxy%'
ORDER BY publish_time
LIMIT 30;

/*
RESULTADO: 4.140 registros de clientes interagindo com a campanha
Inclui: texto completo da mensagem da Lu, uso do cupom CUPOMS26, perguntas sobre o produto.
→ CONFIRMA: os clientes RECEBERAM a campanha. O problema é no rastreamento, não no envio.
*/


-- PASSO 4: Quantificar o alcance real da campanha via conversas
SELECT
  DATE(publish_time)         AS data,
  COUNT(DISTINCT session_id) AS sessoes_unicas,
  COUNT(*)                   AS total_interacoes,
  COUNTIF(LOWER(text) LIKE '%cupoms26%') AS uso_cupom
FROM `projeto.dataset.conversas`
WHERE LOWER(text) LIKE '%cupoms26%'
   OR LOWER(text) LIKE '%galaxy s26%'
   OR LOWER(text) LIKE '%nova linha galaxy%'
GROUP BY 1
ORDER BY 1;

/*
Permite estimar o alcance real da campanha mesmo sem o registro formal
*/


-- PASSO 5: Tentar reconstruir os disparos via conversas (workaround)
-- Identifica sessions onde a primeira mensagem tem o conteúdo da campanha
-- (author = 'request' = mensagem enviada pela plataforma)
WITH primeira_mensagem AS (
  SELECT
    session_id,
    text,
    author,
    publish_time,
    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY publish_time) AS rn
  FROM `projeto.dataset.conversas`
  WHERE LOWER(text) LIKE '%nova linha galaxy s26%'
    OR LOWER(text) LIKE '%cupoms26%'
)
SELECT
  session_id,
  text,
  author,
  publish_time
FROM primeira_mensagem
WHERE rn = 1
  AND author = 'request'
ORDER BY publish_time;

/*
Pode ser usado para estimar quantos disparos foram feitos enquanto o
pipeline não é corrigido e reprocessado.
*/


-- DIAGNÓSTICO FINAL: Comparar as duas campanhas em resumo
SELECT
  'Apple'   AS campanha,
  'crm_cerebro_ads_apple_1903' AS template_esperado,
  'sendtype-835' AS sendtype_esperado,
  COUNT(CASE WHEN template LIKE '%crm_cerebro_ads_apple%'
             AND DATE(publish_time) = '2026-03-19' THEN 1 END) AS registros_em_campanhas,
  'version gravada como 1' AS causa_raiz,
  'Modelagem/Ingestão' AS tipo_problema
FROM `projeto.dataset.campanhas`

UNION ALL

SELECT
  'Samsung' AS campanha,
  'crm_cerebro_galaxys26' AS template_esperado,
  'sendtype-838' AS sendtype_esperado,
  COUNT(CASE WHEN template = 'crm_cerebro_galaxys26' THEN 1 END) AS registros_em_campanhas,
  'CTA não serializado como JSON → deserialização falhou no pipeline' AS causa_raiz,
  'Operacional/Pipeline' AS tipo_problema
FROM `projeto.dataset.campanhas`;
