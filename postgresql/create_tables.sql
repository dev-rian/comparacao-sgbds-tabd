CREATE TABLE clientes (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    telefone VARCHAR(20),
    data_cadastro TIMESTAMPTZ DEFAULT NOW(),
    cpf VARCHAR(14) UNIQUE NOT NULL
);

CREATE TABLE produtos (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    categoria VARCHAR(100) NOT NULL,
    preco NUMERIC(10, 2) NOT NULL,
    estoque INT NOT NULL
);

CREATE TABLE pedidos (
    id SERIAL PRIMARY KEY,
    id_cliente INT REFERENCES clientes(id) ON DELETE SET NULL,
    data_pedido TIMESTAMPTZ DEFAULT NOW(),
    status VARCHAR(50) NOT NULL,
    valor_total NUMERIC(10, 2) NOT NULL
);

CREATE TABLE pedido_itens (
    id SERIAL PRIMARY KEY,
    id_pedido INT REFERENCES pedidos(id) ON DELETE CASCADE,
    id_produto INT REFERENCES produtos(id) ON DELETE RESTRICT,
    quantidade INT NOT NULL,
    preco_unitario NUMERIC(10, 2) NOT NULL
);

CREATE TABLE pagamentos (
    id SERIAL PRIMARY KEY,
    id_pedido INT UNIQUE REFERENCES pedidos(id) ON DELETE CASCADE,
    tipo VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    data_pagamento TIMESTAMPTZ
);

-- Índices
CREATE INDEX idx_cliente_email ON clientes(email);
CREATE INDEX idx_produto_categoria_preco ON produtos(categoria, preco);
CREATE INDEX idx_pedido_cliente_status ON pedidos(id_cliente, status);
CREATE INDEX idx_pagamento_tipo_data ON pagamentos(tipo, data_pagamento);