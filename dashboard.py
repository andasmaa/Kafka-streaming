import time
from datetime import datetime
import altair as alt
import pandas as pd
import streamlit as st
from pyhive import hive

st.set_page_config(
    page_title="Lakehouse — Ventes par catégorie (Gold)",
    page_icon="📊",
    layout="wide",
)

st.markdown(
    """
    <style>
        .block-container {
            padding-top: 1.75rem;
            padding-bottom: 2rem;
        }
        div[data-testid="stMetric"] {
            background-color: #f8fafc;
            border: 1px solid #e2e8f0;
            border-radius: 12px;
            padding: 16px 20px;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
        }
        div[data-testid="stMetricLabel"] {
            font-size: 0.9rem;
            font-weight: 600;
            color: #475569;
        }
        div[data-testid="stMetricValue"] {
            font-size: 1.75rem;
            font-weight: 700;
            color: #0f172a;
        }
        .pipeline-pill {
            display: inline-block;
            background: #eff6ff;
            color: #1d4ed8;
            border: 1px solid #bfdbfe;
            border-radius: 999px;
            padding: 4px 12px;
            font-size: 0.8rem;
            font-weight: 600;
            margin-bottom: 0.5rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


def charger_gold():
    """Lit la table Gold. Plusieurs essais car Gold est réécrit toutes les 20 s."""
    for _ in range(6):
        try:
            conn = hive.Connection(
                host="localhost",
                port=10000,
                username="hive",
                auth="NONE",
            )
            cur = conn.cursor()
            cur.execute("SELECT * FROM gold.category_sales")
            colonnes = [d[0].split(".")[-1] for d in cur.description]
            lignes = cur.fetchall()
            conn.close()
            return [dict(zip(colonnes, l)) for l in lignes]
        except Exception:
            time.sleep(2)
    return None


with st.sidebar:
    st.markdown("### Contrôles")
    auto = st.checkbox("Actualisation automatique (10 s)", value=True)
    if st.button("Actualiser maintenant", use_container_width=True):
        st.rerun()
    st.divider()
    st.caption("Architecture : Kafka → Spark → HDFS → Hive (`gold.category_sales`)")
    st.caption(f"Dernière lecture : {datetime.now().strftime('%H:%M:%S')}")

st.markdown('<div class="pipeline-pill">Streaming Lakehouse • Couche Gold</div>', unsafe_allow_html=True)
st.title("Ventes par catégorie")
st.caption("Source : table Hive `gold.category_sales` interrogée via HiveServer2 (`localhost:10000`)")

data = charger_gold()

if not data:
    st.warning("Aucune donnée dans Gold pour le moment. Vérifier que le job Spark tourne.")
else:
    df = pd.DataFrame(data)
    df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce").fillna(0.0)
    df["invoice_count"] = pd.to_numeric(df["invoice_count"], errors="coerce").fillna(0).astype(int)
    if "average_price" in df.columns:
        df["average_price"] = pd.to_numeric(df["average_price"], errors="coerce").fillna(0.0)

    df = df.sort_values("total_amount", ascending=False).reset_index(drop=True)

    categories_dispo = df["category"].tolist()
    selection = st.sidebar.multiselect(
        "Filtrer les catégories",
        options=categories_dispo,
        default=categories_dispo,
    )
    df_view = df[df["category"].isin(selection)] if selection else df

    total_ventes = float(df_view["total_amount"].sum())
    nb_factures = int(df_view["invoice_count"].sum())
    prix_moyen = (total_ventes / nb_factures) if nb_factures > 0 else 0.0

    c1, c2, c3 = st.columns(3)
    c1.metric("Total des ventes", f"{total_ventes:,.2f}".replace(",", " "))
    c2.metric("Nombre de factures", f"{nb_factures:,}".replace(",", " "))
    c3.metric("Prix moyen", f"{prix_moyen:,.2f}".replace(",", " "))

    st.markdown("")

    cat_order = df_view["category"].tolist()
    g1, g2 = st.columns(2)

    with g1:
        st.subheader("Total des ventes par catégorie")
        chart_sales = (
            alt.Chart(df_view)
            .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
            .encode(
                x=alt.X("category:N", sort=cat_order, title="Catégorie", axis=alt.Axis(labelAngle=0)),
                y=alt.Y("total_amount:Q", title="Total des ventes"),
                color=alt.Color("category:N", legend=None, scale=alt.Scale(scheme="tableau10")),
                tooltip=[
                    alt.Tooltip("category:N", title="Catégorie"),
                    alt.Tooltip("total_amount:Q", title="Total ventes", format=",.2f"),
                    alt.Tooltip("invoice_count:Q", title="Factures", format=","),
                ],
            )
            .properties(height=320)
        )
        st.altair_chart(chart_sales, use_container_width=True)

    with g2:
        st.subheader("Nombre de factures par catégorie")
        chart_count = (
            alt.Chart(df_view)
            .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
            .encode(
                x=alt.X("category:N", sort=cat_order, title="Catégorie", axis=alt.Axis(labelAngle=0)),
                y=alt.Y("invoice_count:Q", title="Nombre de factures"),
                color=alt.Color("category:N", legend=None, scale=alt.Scale(scheme="set2")),
                tooltip=[
                    alt.Tooltip("category:N", title="Catégorie"),
                    alt.Tooltip("invoice_count:Q", title="Factures", format=","),
                    alt.Tooltip("total_amount:Q", title="Total ventes", format=",.2f"),
                ],
            )
            .properties(height=320)
        )
        st.altair_chart(chart_count, use_container_width=True)

    st.subheader("Détail de la table Gold (`gold.category_sales`)")
    st.dataframe(df_view, use_container_width=True, hide_index=True)

if auto:
    time.sleep(10)
    st.rerun()
