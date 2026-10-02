-- Витрина с результатами скоринга.
-- Скрипт выполняется автоматически при ПЕРВОМ старте контейнера Postgres
-- (когда volume с данными ещё пустой).
CREATE TABLE IF NOT EXISTS scores (
    id             BIGSERIAL PRIMARY KEY,
    transaction_id TEXT             NOT NULL UNIQUE,
    score          DOUBLE PRECISION NOT NULL,
    fraud_flag     SMALLINT         NOT NULL,
    created_at     TIMESTAMPTZ      NOT NULL DEFAULT now()
);

-- Индекс для быстрого запроса "последние записи с fraud_flag = 1"
CREATE INDEX IF NOT EXISTS idx_scores_fraud_flag_id ON scores (fraud_flag, id DESC);
