-- ============================================================
-- 03_monitoramento.sql
-- Proposta de Monitoramento Contínuo — Prevenção de falhas silenciosas
-- ============================================================


-- ---------------------------------------------------------------
-- MONITOR 1: Campanhas com campo 'version' fora do padrão
-- Roda diariamente às 07h via BigQuery Scheduled Query
-- Alerta se retornar qualquer linha
-- ---------------------------------------------------------------
SELECT
  template,
  version,
  COUNT(*)           AS total_disparos,
  MIN(publish_time)  AS primeiro_disparo,
  MAX(publish_time)  AS ultimo_disparo
FROM `projeto.dataset.campanhas`
WHERE DATE(publish_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  -- Versões válidas conhecidas:
  AND version NOT LIKE 'sendtype-%'
  AND version NOT IN ('v2')
GROUP BY 1, 2
HAVING total_disparos > 0
ORDER BY total_disparos DESC;

/*
Aciona alerta se:
  - version = '1', 'null', '', ou qualquer string não padronizada
Ação: notificar #data-alerts + abrir ticket para time de engenharia
*/


-- ---------------------------------------------------------------
-- MONITOR 2: Templates novos sem histórico (estreia de campanha)
-- Detecta quando um template aparece pela primeira vez na tabela
-- Útil para confirmar que novas campanhas foram registradas corretamente
-- ---------------------------------------------------------------
WITH templates_novos AS (
  SELECT
    template,
    MIN(DATE(publish_time)) AS primeira_aparicao,
    COUNT(*)                AS total_disparos
  FROM `projeto.dataset.campanhas`
  GROUP BY 1
)
SELECT
  template,
  primeira_aparicao,
  total_disparos
FROM templates_novos
WHERE primeira_aparicao = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
ORDER BY total_disparos DESC;

/*
Ação: confirmar manualmente com o time de CRM se o template novo era esperado.
Se o CRM comunicou uma campanha mas o template não aparece aqui → investigar.
*/


-- ---------------------------------------------------------------
-- MONITOR 3: Campanhas comunicadas pelo CRM sem registro na tabela
-- Requer uma tabela de controle preenchida pelo CRM com antecedência
-- ---------------------------------------------------------------
-- Pré-requisito: tabela `projeto.dataset.campanhas_planejadas`
-- com colunas: template, send_type, data_planejada, responsavel

SELECT
  cp.template              AS template_planejado,
  cp.send_type,
  cp.data_planejada,
  cp.responsavel,
  COUNT(c.session_id)      AS disparos_registrados
FROM `projeto.dataset.campanhas_planejadas` cp
LEFT JOIN `projeto.dataset.campanhas` c
  ON c.template = cp.template
  AND DATE(c.publish_time) = cp.data_planejada
WHERE cp.data_planejada = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY 1, 2, 3, 4
HAVING disparos_registrados = 0;

/*
Retorna campanhas que foram planejadas mas não tiveram nenhum disparo registrado.
Pode indicar:
  - Erro operacional (campanha não foi disparada)
  - Erro de pipeline (disparo ocorreu mas não foi gravado)
*/


-- ---------------------------------------------------------------
-- MONITOR 4: Volume diário de disparos por template (anomalia de volume)
-- Detecta quedas bruscas em relação à média histórica
-- ---------------------------------------------------------------
WITH historico AS (
  SELECT
    template,
    DATE(publish_time)      AS data,
    COUNT(*)                AS disparos_dia
  FROM `projeto.dataset.campanhas`
  WHERE DATE(publish_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY 1, 2
),
media_historica AS (
  SELECT
    template,
    AVG(disparos_dia)    AS media_30d,
    STDDEV(disparos_dia) AS desvio_30d
  FROM historico
  WHERE data < DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) -- exclui ontem do cálculo
  GROUP BY 1
),
ontem AS (
  SELECT template, disparos_dia
  FROM historico
  WHERE data = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)
SELECT
  o.template,
  o.disparos_dia          AS disparos_ontem,
  m.media_30d,
  ROUND(
    (o.disparos_dia - m.media_30d) / NULLIF(m.desvio_30d, 0), 2
  )                       AS z_score
FROM ontem o
JOIN media_historica m USING (template)
WHERE o.disparos_dia < m.media_30d * 0.5  -- queda de mais de 50%
ORDER BY z_score;

/*
Alerta quando uma campanha recorrente tem volume muito abaixo do esperado.
Pode indicar problema silencioso no pipeline.
*/


-- ---------------------------------------------------------------
-- VIEW: Semáforo diário de saúde do pipeline
-- Disponível no dashboard de dados como "health check"
-- ---------------------------------------------------------------
CREATE OR REPLACE VIEW `projeto.dataset.vw_saude_pipeline_campanhas` AS
SELECT
  DATE(publish_time)                                              AS data,
  COUNT(*)                                                        AS total_disparos,
  COUNT(DISTINCT template)                                        AS templates_ativos,
  COUNT(DISTINCT version)                                         AS versoes_distintas,
  COUNTIF(version NOT LIKE 'sendtype-%' AND version NOT IN ('v2'))
                                                                  AS disparos_version_invalida,
  ROUND(
    COUNTIF(version NOT LIKE 'sendtype-%' AND version NOT IN ('v2'))
    / COUNT(*) * 100, 2
  )                                                               AS pct_versao_invalida,
  CASE
    WHEN COUNTIF(version NOT LIKE 'sendtype-%' AND version NOT IN ('v2')) > 0
    THEN '🔴 ALERTA'
    ELSE '🟢 OK'
  END                                                             AS status
FROM `projeto.dataset.campanhas`
GROUP BY 1
ORDER BY 1 DESC;
