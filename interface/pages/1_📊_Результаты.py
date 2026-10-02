"""
Страница результатов скоринга: читает витрину `scores` из Postgres.
"""
import os

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st

PG_DSN = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "fraud"),
    "user": os.getenv("POSTGRES_USER", "fraud"),
    "password": os.getenv("POSTGRES_PASSWORD", "fraud"),
}

LAST_FRAUD_SQL = """
SELECT transaction_id, score, fraud_flag, created_at
FROM scores
WHERE fraud_flag = 1
ORDER BY id DESC
LIMIT 10;
"""

LAST_SCORES_SQL = """
SELECT score
FROM scores
ORDER BY id DESC
LIMIT 100;
"""


def query(sql: str) -> pd.DataFrame:
    with psycopg2.connect(**PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            columns = [d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)


st.title("📊 Результаты скоринга")

if st.button("Посмотреть результаты", type="primary"):
    try:
        fraud_df = query(LAST_FRAUD_SQL)
        scores_df = query(LAST_SCORES_SQL)
    except Exception as e:
        st.error(f"Не удалось получить данные из Postgres: {e}")
        st.stop()

    st.subheader("Последние 10 транзакций с fraud_flag = 1")
    if fraud_df.empty:
        st.info("Пока нет транзакций, помеченных как фрод.")
    else:
        st.dataframe(fraud_df, use_container_width=True, hide_index=True)

    st.subheader(f"Распределение скоров последних {len(scores_df)} транзакций")
    if scores_df.empty:
        st.info("В базе пока нет транзакций. Отправьте файл на главной странице.")
    else:
        chart = alt.Chart(scores_df).mark_bar().encode(
            x=alt.X("score:Q", bin=alt.Bin(maxbins=20, extent=[0, 1]), title="Скор модели"),
            y=alt.Y("count():Q", title="Количество транзакций"),
        )
        st.altair_chart(chart, use_container_width=True)
