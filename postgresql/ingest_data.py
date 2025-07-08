# ingest_postgres.py
import psycopg2
import psycopg2.extras
from faker import Faker
import random
import time
from datetime import datetime

# --- CONFIGURAÇÃO ---
DB_NAME = "techmarket_db"
DB_USER = "postgres"
DB_PASS = "35263646"
DB_HOST = "localhost"
DB_PORT = "5432"

# Núm. de registrs
NUM_CLIENTES = 20000
NUM_PRODUTOS = 5000
NUM_PEDIDOS = 30000
NUM_PAGAMENTOS = 30000

# --- CONEXÃO ---
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
)
cursor = conn.cursor()
fake = Faker('pt_BR')

print("Iniciando a geração e ingestão de dados para PostgreSQL...")
start_time = time.time()

try:
    # Clientes
    print(f"Gerando e inserindo {NUM_CLIENTES} clientes...")
    clientes = [
        (fake.name(), fake.unique.email(), fake.phone_number(), fake.date_time_this_year(), fake.cpf())
        for _ in range(NUM_CLIENTES)
    ]
    insert_query = "INSERT INTO clientes (nome, email, telefone, data_cadastro, cpf) VALUES %s RETURNING id;"
    psycopg2.extras.execute_values(cursor, insert_query, clientes, page_size=1000)
    cliente_ids = [row[0] for row in cursor.fetchall()]

    # Produtos
    print(f"Gerando e inserindo {NUM_PRODUTOS} produtos...")
    categorias_produto = ['Eletrônicos', 'Móveis', 'Livros', 'Roupas', 'Ferramentas', 'Esportes']
    produtos = [
        (fake.company(), random.choice(categorias_produto), round(random.uniform(10.0, 2000.0), 2), random.randint(0, 100))
        for _ in range(NUM_PRODUTOS)
    ]
    insert_query = "INSERT INTO produtos (nome, categoria, preco, estoque) VALUES %s RETURNING id;"
    psycopg2.extras.execute_values(cursor, insert_query, produtos, page_size=1000)
    produto_ids_com_preco = [(row[0],) for row in cursor.fetchall()]

    # Mapear IDs para preços 
    produto_map = {produto_ids_com_preco[i][0]: produtos[i] for i in range(len(produtos))}

    # Gerar Pedidos, Itens e Pagamentos
    print(f"Gerando {NUM_PEDIDOS} pedidos, seus itens e pagamentos...")
    pedidos_data = []
    pedido_itens_data = []
    pagamentos_data = []

    for i in range(NUM_PEDIDOS):
        cliente_id = random.choice(cliente_ids)
        # Placeholder para valor total e id
        pedidos_data.append((i + 1, cliente_id, fake.date_time_this_year(), random.choice(['processando', 'enviado', 'entregue']), 0.0))

        # Adicionar itens ao pedido
        num_itens = random.randint(1, 5)
        valor_total_pedido = 0.0
        produtos_escolhidos_ids = random.sample(list(produto_map.keys()), num_itens)

        for produto_id in produtos_escolhidos_ids:
            produto_info = produto_map[produto_id]
            preco_unitario = produto_info[2]
            quantidade = random.randint(1, 3)
            pedido_itens_data.append((i + 1, produto_id, quantidade, preco_unitario))
            valor_total_pedido += preco_unitario * quantidade

        # Atualizar valor total
        pedidos_data[i] = (pedidos_data[i][0], pedidos_data[i][1], pedidos_data[i][2], pedidos_data[i][3], round(valor_total_pedido, 2))
        
        # Gerar pagamento correspondente
        pagamentos_data.append((
            i + 1,
            random.choice(['cartão', 'pix', 'boleto']),
            'aprovado',
            fake.date_time_this_year()
        ))

    # Inserir Pedidos
    print("Inserindo pedidos...")
    # Precisamos do id explícito para poder ligar itens e pagamentos antes do commit
    cursor.execute("CREATE TEMPORARY TABLE temp_pedidos (id INT, id_cliente INT, data_pedido TIMESTAMPTZ, status VARCHAR, valor_total NUMERIC);")
    psycopg2.extras.execute_values(cursor, "INSERT INTO temp_pedidos (id, id_cliente, data_pedido, status, valor_total) VALUES %s;", pedidos_data, page_size=1000)
    cursor.execute("""
        INSERT INTO pedidos (id, id_cliente, data_pedido, status, valor_total)
        SELECT id, id_cliente, data_pedido, status, valor_total FROM temp_pedidos;
    """)
    # Resetar a sequência do serial para o valor correto
    cursor.execute(f"SELECT setval('pedidos_id_seq', {NUM_PEDIDOS}, true);")

    # Inserir Itens e Pagamentos
    print("Inserindo itens de pedido e pagamentos...")
    insert_query_itens = "INSERT INTO pedido_itens (id_pedido, id_produto, quantidade, preco_unitario) VALUES %s;"
    psycopg2.extras.execute_values(cursor, insert_query_itens, pedido_itens_data, page_size=2000)
    
    insert_query_pagamentos = "INSERT INTO pagamentos (id_pedido, tipo, status, data_pagamento) VALUES %s;"
    psycopg2.extras.execute_values(cursor, insert_query_pagamentos, pagamentos_data, page_size=2000)

    conn.commit()

except (Exception, psycopg2.Error) as error:
    print("Erro durante a transação, revertendo...", error)
    conn.rollback()

finally:
    end_time = time.time()
    cursor.close()
    conn.close()
    print(f"\nConexão com PostgreSQL fechada. Tempo total de ingestão: {end_time - start_time:.2f} segundos.")
