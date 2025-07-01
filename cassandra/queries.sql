-- Q1:
SELECT * FROM pedidos_por_cliente 
WHERE cliente_email = 'cliente@email.com' 
LIMIT 3;

-- Q2:
SELECT nome_produto, preco FROM produtos_por_categoria 
WHERE categoria = 'Eletrônicos';

-- Q3:
SELECT * FROM pedidos_por_cliente 
WHERE cliente_email = 'cliente@email.com' AND status = 'entregue'; -- RUIM! Exigiria ALLOW FILTERING.
-- Q4:
SELECT produto_id, quantidade_vendida FROM vendas_por_produto 
LIMIT 1000;

-- Q5:
SELECT pedido_id, valor_total, data_pagamento FROM pagamentos_por_tipo_e_mes 
WHERE tipo_pagamento = 'pix' AND ano_mes = '2023-11';

-- Q6: 
SELECT * FROM gasto_cliente_por_mes 
WHERE cliente_id = ? AND ano_mes IN ('2023-11', '2023-10', '2023-09');