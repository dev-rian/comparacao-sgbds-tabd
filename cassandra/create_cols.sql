-- Tabela para Q1 e Q3
CREATE TABLE pedidos_por_cliente (
    cliente_email text,
    data_pedido timestamp,
    pedido_id uuid,
    status text,
    valor_total decimal,
    itens_pedido list<text>,
    PRIMARY KEY (cliente_email, data_pedido)
) WITH CLUSTERING ORDER BY (data_pedido DESC);

-- Tabela para Q2
CREATE TABLE produtos_por_categoria (
    categoria text,
    preco decimal,
    produto_id uuid,
    nome_produto text,
    estoque int,
    PRIMARY KEY (categoria, preco, produto_id)
) WITH CLUSTERING ORDER BY (preco ASC);

-- Tabela para Q5
CREATE TABLE pagamentos_por_tipo_e_mes (
    tipo_pagamento text,
    ano_mes text,
    data_pagamento timestamp,
    pedido_id uuid,
    valor_total decimal,
    PRIMARY KEY ((tipo_pagamento, ano_mes), data_pagamento)
);

-- Tabela para Q4 e Q6
CREATE TABLE vendas_por_produto (
    produto_id uuid PRIMARY KEY,
    quantidade_vendida counter
);

CREATE TABLE gasto_cliente_por_mes (
    cliente_id uuid,
    ano_mes text, 
    total_gasto counter,
    PRIMARY KEY (cliente_id, ano_mes)
);

