import psycopg2
import psycopg2.extras
from faker import Faker
import random
import time
from datetime import datetime
import uuid

# --- CONFIGURAÇÃO ---
DB_NAME = "techmarket_db"
DB_USER = "postgres"
DB_PASS = "35263646" 
DB_HOST = "localhost"
DB_PORT = "5432"

NUM_CLIENTES = 20000
NUM_PRODUTOS = 5000
NUM_PEDIDOS = 30000

# --- CONEXÃO ---
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
)
cursor = conn.cursor()
fake = Faker('pt_BR')

print("Iniciando a geração de dados...")

# --- GERAÇÃO DE DADOS EM MEMÓRIA ---
clientes = [
    (fake.name(), fake.unique.email(), fake.phone_number(), datetime.now(), fake.cpf())
    for _ in range(NUM_CLIENTES)
]
print(f"{len(clientes)} clientes gerados.")

categorias_produto = ['Eletrônicos', 'Móveis', 'Livros', 'Roupas', 'Ferramentas', 'Esportes']
produtos = [
    (fake.company(), random.choice(categorias_produto), round(random.uniform(10.0, 2000.0), 2), random.randint(0, 100))
    for _ in range(NUM_PRODUTOS)
]
print(f"{len(produtos)} produtos gerados.")

# --- INGESTÃO ---
start_time = time.time()

try:
    print("\nIniciando ingestão no PostgreSQL...")

    # 1. Inserir Clientes e obter IDs
    print("Inserindo clientes...")
    insert_query = "INSERT INTO clientes (nome, email, telefone, data_cadastro, cpf) VALUES %s RETURNING id;"
    psycopg2.extras.execute_values(cursor, insert_query, clientes, page_size=1000)
    cliente_ids = [row[0] for row in cursor.fetchall()]
    print(f"-> {len(cliente_ids)} clientes inseridos.")
    
    # 2. Inserir Produtos e obter IDs
    print("Inserindo produtos...")
    insert_query = "INSERT INTO produtos (nome, categoria, preco, estoque) VALUES %s RETURNING id;"
    psycopg2.extras.execute_values(cursor, insert_query, produtos, page_size=1000)
    produto_ids = [row[0] for row in cursor.fetchall()]
    print(f"-> {len(produto_ids)} produtos inseridos.")

    # 3. Gerar e Inserir Pedidos
    print("Gerando e inserindo pedidos...")
    pedidos_data = []
    for _ in range(NUM_PEDIDOS):
        cliente_id = random.choice(cliente_ids)
        num_itens = random.randint(1, 5)
        # Placeholder para valor total, será atualizado depois
        pedidos_data.append((cliente_id, fake.date_time_this_year(), random.choice(['processando', 'enviado', 'entregue']), 0.0))
    
    insert_query = "INSERT INTO pedidos (id_cliente, data_pedido, status, valor_total) VALUES %s RETURNING id;"
    psycopg2.extras.execute_values(cursor, insert_query, pedidos_data, page_size=1000)
    pedido_ids = [row[0] for row in cursor.fetchall()]
    print(f"-> {len(pedido_ids)} pedidos inseridos (com valor_total=0).")
    
    # 4. Gerar Itens de Pedido, Pagamentos e atualizar Pedidos
    print("Gerando e inserindo itens, pagamentos e atualizando valores...")
    pedido_itens_data = []
    pagamentos_data = []
    pedidos_update_data = []

    for pedido_id in pedido_ids:
        valor_total_pedido = 0
        num_itens = random.randint(1, 5)
        for _ in range(num_itens):
            produto_idx = random.randint(0, len(produtos) - 1)
            produto_id_escolhido = produto_ids[produto_idx]
            preco_unitario = produtos[produto_idx][2]
            quantidade = random.randint(1, 3)
            pedido_itens_data.append((pedido_id, produto_id_escolhido, quantidade, preco_unitario))
            valor_total_pedido += preco_unitario * quantidade

        pedidos_update_data.append((round(valor_total_pedido, 2), pedido_id))

        pagamentos_data.append((
            pedido_id, 
            random.choice(['cartão', 'pix', 'boleto']), 
            'aprovado', 
            fake.date_time_this_year()
        ))
        
    # Inserção em lote
    insert_query_itens = "INSERT INTO pedido_itens (id_pedido, id_produto, quantidade, preco_unitario) VALUES %s;"
    psycopg2.extras.execute_values(cursor, insert_query_itens, pedido_itens_data, page_size=1000)
    print(f"-> {len(pedido_itens_data)} itens de pedido inseridos.")
    
    insert_query_pagamentos = "INSERT INTO pagamentos (id_pedido, tipo, status, data_pagamento) VALUES %s;"
    psycopg2.extras.execute_values(cursor, insert_query_pagamentos, pagamentos_data, page_size=1000)
    print(f"-> {len(pagamentos_data)} pagamentos inseridos.")

    update_query_pedidos = "UPDATE pedidos SET valor_total = data.valor_total FROM (VALUES %s) AS data(valor_total, id) WHERE pedidos.id = data.id;"
    psycopg2.extras.execute_values(cursor, update_query_pedidos, pedidos_update_data, page_size=1000)
    print(f"-> {len(pedidos_update_data)} valores totais de pedidos atualizados.")
    
    conn.commit()

except (Exception, psycopg2.Error) as error:
    print("Erro durante a transação, revertendo...", error)
    conn.rollback()

finally:
    end_time = time.time()
    cursor.close()
    conn.close()
    print(f"\nConexão com PostgreSQL fechada. Tempo total de ingestão: {end_time - start_time:.2f} segundos.")