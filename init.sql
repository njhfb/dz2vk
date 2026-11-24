-- Таблица-справочник, за которой следим
CREATE TABLE accounts (
    id         SERIAL PRIMARY KEY,
    owner      VARCHAR(100) NOT NULL,
    status     VARCHAR(20)  NOT NULL,
    updated_at TIMESTAMP    NOT NULL DEFAULT now()
);

-- Без этого в UPDATE-событиях Debezium поле "before" будет почти пустым,
-- а для SCD Type 2 нужно видеть старые значения
ALTER TABLE accounts REPLICA IDENTITY FULL;

INSERT INTO accounts (owner, status) VALUES
    ('Иванов И.И.', 'active'),
    ('Петрова А.С.', 'active'),
    ('Сидоров П.К.', 'blocked');
