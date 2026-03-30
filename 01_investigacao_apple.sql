
-- PASSO 1: Verificar se o template existe na tabela campanhas
-- e qual valor está no campo version
SELECT
  template,
  version,
  COUNT(*)           AS total_disparos,
  DATE(publish_time) AS data_disparo
FROM `temp_bq.campanhas`
WHERE template IN ('crm_cerebro_ads_apple_1903', 'crm_cerebro_galaxys26')
   OR template LIKE '%crm_cerebro_ads_apple%'
GROUP BY 1, 2, 4
ORDER BY 4;

/*
Resultado: O template existe — 149 disparos em 19/03 e 92 em 20/03. Porém o campo version está gravado como '1',
não 'sendtype-835'.
*/

-- PASSO 2: Verificar todos os send types (versions) que existem na tabela campanhas:
SELECT
  version,
  COUNT(*) AS total
FROM `temp_bq.campanhas`
GROUP BY 1
ORDER BY 2 DESC;
/*
Resultado: Não localizado templates 'sendtype-835'.
*/

-- PASSO 3: Verificar o JOIN com conversas:
SELECT
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

/*
Resultado: 149 sessions da Apple sem nenhum match em conversas (0/149). 
O JOIN entre as tabelas funciona — o problema está em como o dashboard filtra por Send Type.
*/


-- PASSO 3: Confirmar que o dashboard filtra por sendtype e está quebrando
-- Simula o filtro que provavelmente está no dashboard
SELECT COUNT(*) AS resultado_com_filtro_dashboard
FROM `temp_bq.campanhas`
WHERE version = 'sendtype-835'
  AND DATE(publish_time) = '2026-03-19';

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
FROM `temp_bq.campanhas`
WHERE template LIKE '%apple%'
GROUP BY 1, 2, 4
ORDER BY 4;

/*
Resultado:
PADRÃO NORMAL (campanhas Natal):
  apple7_natal_2025    | sendtype-758 | 11 | 2026-...
  apple16e_natal_2025  | sendtype-757 | 5  | 2026-...

PADRÃO QUEBRADO (campanha Cérebro):
  crm_cerebro_ads_apple_* | 1 | N | 2026-03-19
*/
