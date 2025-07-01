-- Q1:
SELECT * FROM pedidos 
WHERE id_cliente = (SELECT id FROM clientes WHERE email = 'cliente@email.com') 
ORDER BY data_pedido DESC LIMIT 3;

-- Q2:
SELECT nome, preco FROM produtos 
WHERE categoria = 'Eletrônicos' 
ORDER BY preco ASC;

-- Q3:
SELECT P.* FROM 
pedido P JOIN clientes C ON P.id_cliente = C.id 
WHERE C.email = 'cliente@email.com' AND P.status = 'entregue';

-- Q4:
SELECT PR.nome, SUM(PI.quantidade) as total_vendido FROM pedidos_itens PI 
JOIN produtos PR ON PI.id_produto = PR.id 
GROUP BY PR.nome 
ORDER BY total_vendido DESC LIMIT 5;

-- Q5:
SELECT id, id_pedido, data_pagamento FROM pagamentos 
WHERE tipo = 'pix' AND data_pagamento >= NOW() - INTERVAL '1 month';

-- Q6:
SELECT SUM(valor_total) FROM pedidos 
WHERE id_cliente = 123 AND data_pedido >= NOW() - INTERVAL '3 months';