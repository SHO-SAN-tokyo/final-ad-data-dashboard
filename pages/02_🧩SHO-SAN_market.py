import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery

# ------------------------------------------------------------
# 0. ページ設定
# ------------------------------------------------------------
st.set_page_config(page_title="カテゴリ×都道府県 達成率モニター", layout="wide")
st.title("🧩 SHO‑SAN market")
st.subheader("📊 カテゴリ × 都道府県 キャンペーン達成率モニター")

# ------------------------------------------------------------
# 1. データ読み込み
# ------------------------------------------------------------
cred = dict(st.secrets["connections"]["bigquery"])
cred["private_key"] = cred["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(cred)

@st.cache_data(show_spinner=False)
def load_data():
    query = """
        SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Market_Monthly_Evaluated_View`
    """
    return client.query(query).to_dataframe()

df = load_data()

# ------------------------------------------------------------
# 2. 前処理
# ------------------------------------------------------------
df["配信月_dt"] = pd.to_datetime(df["配信月"] + "-01", errors="coerce")
df["配信月"] = df["配信月_dt"].dt.strftime("%Y/%m")

# 目標CPAに対する達成評価列を追加
df["目標CPA評価"] = df.apply(
    lambda row: "◎" if pd.notna(row["目標CPA"]) and pd.notna(row["CPA"]) and row["CPA"] <= row["目標CPA"] else "×",
    axis=1
)

# ------------------------------------------------------------
# 3. フィルター
# ------------------------------------------------------------
col1, col2, col3 = st.columns(3)
with col1:
    cat = st.selectbox("📁 カテゴリ", ["すべて"] + sorted(df["カテゴリ"].dropna().unique()))
with col2:
    pref = st.selectbox("🗾 都道府県", ["すべて"] + sorted(df["都道府県"].dropna().unique()))
with col3:
    obj = st.selectbox("🎯 広告目的", ["すべて"] + sorted(df["広告目的"].dropna().unique()))

if cat != "すべて":
    df = df[df["カテゴリ"] == cat]
if pref != "すべて":
    df = df[df["都道府県"] == pref]
if obj != "すべて":
    df = df[df["広告目的"] == obj]

# ------------------------------------------------------------
# 4. 表示テーブル（達成率一覧）
# ------------------------------------------------------------
st.markdown("### 📋 達成率一覧")

表示列 = [
    "配信月", "都道府県", "カテゴリ", "広告目的", "CampaignName",
    "CPA", "CPA_best", "CPA_評価",
    "CVR", "CVR_best", "CVR_評価",
    "CTR", "CTR_best", "CTR_評価",
    "CPC", "CPC_best", "CPC_評価",
    "CPM", "CPM_best", "CPM_評価",
    "目標CPA", "目標CPA評価"
]

# 整形（%系指標、配信月）
df_fmt = df[表示列].copy()
for col in ["CVR", "CVR_best", "CTR", "CTR_best"]:
    df_fmt[col] = df_fmt[col].apply(lambda x: f"{x:.1%}" if pd.notna(x) else "")
for col in ["CPA", "CPA_best", "CPC", "CPC_best", "CPM", "CPM_best", "目標CPA"]:
    df_fmt[col] = df_fmt[col].apply(lambda x: f"¥{x:,.0f}" if pd.notna(x) else "")

st.dataframe(df_fmt.sort_values(["配信月", "都道府県", "CampaignName"]), use_container_width=True, hide_index=True)

# ------------------------------------------------------------
# 5. 月別推移グラフ（指標ごとに分けて表示）
# ------------------------------------------------------------
st.markdown("### 📈 月別推移グラフ（指標別）")
指標群 = ["CPA", "CVR", "CTR", "CPC", "CPM"]

for 指標 in 指標群:
    st.markdown(f"#### 📉 {指標} 推移")
    df_plot = (
        df.groupby("配信月_dt")
          .agg(実績値=(指標, "mean"), 目標値=(f"{指標}_best", "mean"))
          .reset_index()
    )
    fig = px.line(df_plot, x="配信月_dt", y=["実績値", "目標値"], markers=True)
    fig.update_layout(yaxis_title=指標, xaxis_title="配信月", height=400)
    st.plotly_chart(fig, use_container_width=True)
