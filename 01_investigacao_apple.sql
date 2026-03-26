-- ============================================================
-- 01_investigacao_apple.sql
-- Investigação: Campanha Apple (Send Type 835 / crm_cerebro_ads_apple_1903)
-- ============================================================

-- PASSO 1: Verificar se o template existe na tabela campanhas
-- e qual valor está no campo version
SELECT
  template,
  version,
  COUNT(*)          AS total_disparos,
  DATE(publish_time) AS data_disparo
FROM `projeto.dataset.campanhas`
WHERE template LIKE '%crm_cerebro_ads_apple%'
GROUP BY 1, 2, 4
ORDER BY 4;

/*
RESULTADO ESPERADO:
template                        | version | total_disparos | data_disparo
crm_cerebro_ads_apple_1303      | 1       | 103            | 2026-03-19
crm_cerebro_ads_apple_1003      | 1       | 44             | 2026-03-19
crm_cerebro_ads_apple_at        | 1       | 2              | 2026-03-19
...

⚠️ version = '1' ao invés de 'sendtype-835'
*/


-- PASSO 2: Verificar se as sessions aparecem em conversas (JOIN check)
SELECT
  c.session_id,
  c.template,
  c.version,
  cv.text,
  cv.author,
  cv.publish_time AS publish_time_conversa
FROM `projeto.dataset.campanhas` c
LEFT JOIN `projeto.dataset.conversas` cv
  ON c.session_id = cv.session_id
WHERE c.template LIKE '%crm_cerebro_ads_apple%'
  AND DATE(c.publish_time) = '2026-03-19'
ORDER BY c.publish_time
LIMIT 50;

/*
RESULTADO: text e author são NULL para todos os registros
→ O JOIN funciona, mas nenhuma conversa foi iniciada ainda
   (ou as sessions não se cruzam porque o problema é a filtragem por version)
*/


-- PASSO 3: Confirmar que o dashboard filtra por sendtype e está quebrando
-- Simula o filtro que provavelmente está no dashboard
SELECT
  template,
  version,
  COUNT(*) AS total
FROM `projeto.dataset.campanhas`
WHERE version = 'sendtype-835'              -- filtro do dashboard
  AND DATE(publish_time) = '2026-03-19'
GROUP BY 1, 2;

/*
RESULTADO: 0 registros
→ CONFIRMA: o dashboard não acha os dados porque version = '1', não 'sendtype-835'
*/


-- PASSO 4: Confirmar a inconsistência comparando com outras campanhas Apple
-- que têm o campo version preenchido corretamente
SELECT
  template,
  version,
  COUNT(*) AS total,
  DATE(publish_time) AS data
FROM `projeto.dataset.campanhas`
WHERE template LIKE '%apple%'
GROUP BY 1, 2, 4
ORDER BY 4;

/*
PADRÃO NORMAL (campanhas Natal):
  apple7_natal_2025    | sendtype-758 | 11 | 2026-...
  apple16e_natal_2025  | sendtype-757 | 5  | 2026-...

PADRÃO QUEBRADO (campanha Cérebro):
  crm_cerebro_ads_apple_* | 1 | N | 2026-03-19
*/


-- CORREÇÃO SUGERIDA (para hotfix no dashboard enquanto o pipeline é ajustado):
-- Adicionar condição alternativa no filtro de send type
SELECT
  c.session_id,
  c.template,
  c.version,
  cv.session_id AS conv_session,
  cv.author,
  cv.publish_time AS data_interacao
FROM `projeto.dataset.campanhas` c
LEFT JOIN `projeto.dataset.conversas` cv
  ON c.session_id = cv.session_id
WHERE (
    c.version = 'sendtype-835'              -- filtro original
    OR (
      c.template LIKE '%crm_cerebro_ads_apple%'
      AND DATE(c.publish_time) = '2026-03-19'   -- hotfix temporário
    )
  )
ORDER BY c.publish_time;
