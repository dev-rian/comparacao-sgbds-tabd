# ingest_cassandra.py
from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import PreparedStatement
from faker import Faker
import random
import time
import uuid
from datetime import datetime

# --- CONFIGURAÇÃO ---
CASSANDRA_HOSTS = ['127.0.0.1']
KEYSPACE = "techmarket_db"

NUM_CLIENTES = 20000
NUM_PRODUTOS = 5000
NUM_PEDIDOS = 30000

# --- CONEXÃO ---
cluster = Cluster(CASSANDRA_HOSTS)
session = cluster.connect()
session.set_keyspace(KEYSPACE)
fake = Faker('pt_BR')

print("Iniciando a geração de dados...")

# --- GERAÇÃO DE DADOS EM MEMÓRIA ---
clientes = [{
    'id': uuid.uuid4(),
    'email': fake.unique.email()
} for _ in range(NUM_CLIENTES)]
print(f"{len(clientes)} clientes gerados.")

categorias_produto = ['Eletrônicos', 'Móveis', 'Livros', 'Roupas', 'Ferramentas', 'Esportes']
produtos = [{
    'id': uuid.uuid4(),
    'nome': fake.company(),
    'categoria': random.choice(categorias_produto),
    'preco': round(random.uniform(10.0, 2000.0), 2)
} for _ in range(NUM_PRODUTOS)]
print(f"{len(produtos)} produtos gerados.")


# --- PREPARANDO STATEMENTS (BOA PRÁTICA DE PERFORMANCE) ---
print("\nPreparando statements CQL...")
insert_pedido_cliente = session.prepare("INSERT INTO pedidos_por_cliente (cliente_email, data_pedido, pedido_id, status, valor_total, itens_pedido) VALUES (?, ?, ?, ?, ?, ?)")
insert_produto_categoria = session.prepare("INSERT INTO produtos_por_categoria (categoria, preco, produto_id, nome_produto, estoque) VALUES (?, ?, ?, ?, ?)")
insert_pagamento_pix = session.prepare("INSERT INTO pagamentos_por_tipo_e_mes (tipo_pagamento, ano_mes, data_pagamento, pedido_id, valor_total) VALUES (?, ?, ?, ?, ?)")
update_vendas_produto = session.prepare("UPDATE vendas_por_produto SET quantidade_vendida = quantidade_vendida + ? WHERE produto_id = ?")
update_gasto_cliente = session.prepare("UPDATE gasto_cliente_por_mes SET total_gasto = total_gasto + ? WHERE cliente_id = ? AND ano_mes = ?")


# --- INGESTÃO ---
start_time = time.time()
print("\nIniciando ingestão no Cassandra...")

# 1. Inserir Produtos
# (Poderia ser em lote, mas faremos individual para simplicidade de demonstração)
print("Inserindo produtos...")
for p in produtos:
    session.execute(insert_produto_categoria, (p['categoria'], p['preco'], p['id'], p['nome'], random.randint(0, 100)))
print(f"-> {len(produtos)} produtos inseridos.")

# 2. Inserir Pedidos (simulando eventos de compra)
print("Processando e inserindo eventos de pedido...")
for i in range(NUM_PEDIDOS):
    # Escolha de dados aleatórios
    cliente = random.choice(clientes)
    data_pedido_dt = fake.date_time_this_year()
    ano_mes_str = data_pedido_dt.strftime("%Y-%m")
    
    # Criar itens e calcular total
    itens_para_texto = []
    valor_total_pedido = 0.0
    itens_do_pedido = []
    num_itens = random.randint(1, 5)
    for _ in range(num_itens):
        produto = random.choice(produtos)
        quantidade = random.randint(1, 3)
        itens_para_texto.append(f"{quantidade}x {produto['nome']}")
        valor_total_pedido += produto['preco'] * quantidade
        itens_do_pedido.append({'produto': produto, 'quantidade': quantidade})

    valor_total_pedido = round(valor_total_pedido, 2)
    
    # Batch para as inserções (sem contadores)
    batch = BatchStatement()
    pedido_id = uuid.uuid4()
    
    batch.add(insert_pedido_cliente, (
        cliente['email'],
        data_pedido_dt,
        pedido_id,
        random.choice(['processando', 'enviado', 'entregue']),
        valor_total_pedido,
        itens_para_texto
    ))
    
    tipo_pagamento = random.choice(['cartão', 'pix', 'boleto'])
    if tipo_pagamento == 'pix':
        batch.add(insert_pagamento_pix, ('pix', ano_mes_str, data_pedido_dt, pedido_id, valor_total_pedido))
    
    session.execute(batch)
    
    # Execuções separadas para os contadores (não podem estar em lote com INSERTs)
    session.execute(update_gasto_cliente, (valor_total_pedido, cliente['id'], ano_mes_str))
    for item in itens_do_pedido:
        session.execute(update_vendas_produto, (item['quantidade'], item['produto']['id']))

    if (i + 1) % 5000 == 0:
        print(f"-> {i + 1}/{NUM_PEDIDOS} eventos de pedido processados...")

end_time = time.time()
cluster.shutdown()
print(f"\nConexão com Cassandra fechada. Tempo total de ingestão: {end_time - start_time:.2f} segundos.")