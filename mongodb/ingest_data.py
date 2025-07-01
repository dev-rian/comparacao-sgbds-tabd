# ingest_mongodb.py
from pymongo import MongoClient
from faker import Faker
import random
import time
from datetime import datetime

# --- CONFIGURAÇÃO ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "techmarket_db"

NUM_CLIENTES = 20000
NUM_PRODUTOS = 5000
NUM_PEDIDOS = 30000

# --- CONEXÃO ---
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
fake = Faker('pt_BR')

# Limpa coleções antigas para uma nova execução
db.clientes.delete_many({})
db.produtos.delete_many({})
db.pedidos.delete_many({})

print("Iniciando a geração de dados...")

# --- GERAÇÃO DE DADOS ---
clientes = [{
    "nome": fake.name(),
    "email": fake.unique.email(),
    "telefone": fake.phone_number(),
    "data_cadastro": datetime.now(),
    "cpf": fake.cpf()
} for _ in range(NUM_CLIENTES)]
print(f"{len(clientes)} clientes gerados.")

categorias_produto = ['Eletrônicos', 'Móveis', 'Livros', 'Roupas', 'Ferramentas', 'Esportes']
produtos = [{
    "nome": fake.company(),
    "categoria": random.choice(categorias_produto),
    "preco": round(random.uniform(10.0, 2000.0), 2),
    "estoque": random.randint(0, 100)
} for _ in range(NUM_PRODUTOS)]
print(f"{len(produtos)} produtos gerados.")

# --- INGESTÃO ---
start_time = time.time()

print("\nIniciando ingestão no MongoDB...")

# 1. Inserir Clientes e Produtos em lote
print("Inserindo clientes e produtos...")
result_clientes = db.clientes.insert_many(clientes, ordered=False)
cliente_ids = result_clientes.inserted_ids
print(f"-> {len(cliente_ids)} clientes inseridos.")

result_produtos = db.produtos.insert_many(produtos, ordered=False)
produto_ids = result_produtos.inserted_ids
print(f"-> {len(produto_ids)} produtos inseridos.")

# 2. Gerar e Inserir Pedidos com dados embutidos
print("Gerando e inserindo pedidos (com dados embutidos)...")
pedidos_docs = []
for i in range(NUM_PEDIDOS):
    itens_pedido = []
    valor_total_pedido = 0.0
    num_itens = random.randint(1, 5)

    for _ in range(num_itens):
        produto_escolhido = random.choice(produtos)
        quantidade = random.randint(1, 3)
        preco_unitario = produto_escolhido['preco']
        
        itens_pedido.append({
            "produto_id": random.choice(produto_ids), # Referência
            "nome_produto": produto_escolhido['nome'], # Denormalizado
            "preco_unitario": preco_unitario,
            "quantidade": quantidade
        })
        valor_total_pedido += preco_unitario * quantidade
    
    cliente_escolhido = random.choice(clientes)
    pedido = {
        "cliente_id": random.choice(cliente_ids),
        "data_pedido": fake.date_time_this_year(),
        "status": random.choice(['processando', 'enviado', 'entregue']),
        "itens": itens_pedido,
        "valor_total": round(valor_total_pedido, 2),
        "pagamento": {
            "tipo": random.choice(['cartão', 'pix', 'boleto']),
            "status": 'aprovado',
            "data_pagamento": fake.date_time_this_year()
        }
    }
    pedidos_docs.append(pedido)

# Inserção final em lote
db.pedidos.insert_many(pedidos_docs, ordered=False)
print(f"-> {len(pedidos_docs)} pedidos inseridos.")

end_time = time.time()
client.close()
print(f"\nConexão com MongoDB fechada. Tempo total de ingestão: {end_time - start_time:.2f} segundos.")