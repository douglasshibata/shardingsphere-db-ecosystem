-- Script de Inicialização para o Fragmento 1 (Database: ds_1)
-- ---------------------------------------------------------
-- Cria as tabelas físicas associadas a este shard.

-- Tabela Física: orders_0
CREATE TABLE IF NOT EXISTS orders_0 (
    order_id BIGINT PRIMARY KEY,
    user_id INT NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    cpf_cipher VARCHAR(255) NOT NULL,
    credit_card_cipher VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela Física: orders_1
CREATE TABLE IF NOT EXISTS orders_1 (
    order_id BIGINT PRIMARY KEY,
    user_id INT NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    cpf_cipher VARCHAR(255) NOT NULL,
    credit_card_cipher VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para otimização de consultas
CREATE INDEX IF NOT EXISTS idx_orders_0_user ON orders_0(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_1_user ON orders_1(user_id);
