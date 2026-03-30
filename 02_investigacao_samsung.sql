-- 02_investigacao_samsung.sql -
-- PASSO 1: Verificar se o template existe na tabela campanhas
SELECT COUNT(*)
FROM `temp_bq.campanhas`
WHERE template = 'crm_cerebro_galaxys26';

/*
RESULTADO: 0 registros
→ A campanha simplesmente não existe na tabela campanhas
*/


-- PASSO 2: Verificar se chegou alguma resposta de cliente em conversas
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

/*
RESULTADO: 4.140 registros de clientes interagindo com a campanha
Inclui: texto completo da mensagem da Lu, uso do cupom CUPOMS26, perguntas sobre o produto.
→ CONFIRMA: os clientes RECEBERAM a campanha. O problema é no rastreamento, não no envio.
*/


-- PASSO 3: Investigar os logs do Omnichannel
SELECT
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

/*
Resultado: Muitas entradas de log com o erro: It is not a JSON type and cannot be deserialized: + mensagem.
*/


-- PASSO 4: Confirmar que o envio chegou ao cliente:
SELECT
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

/*
Resultado: Logs com Successfully sent message confirmam que o Omnichannel entregou as mensagens aos destinatários. 
O erro ocorreu na camada de callback/tracking, não no envio em si.
*/
