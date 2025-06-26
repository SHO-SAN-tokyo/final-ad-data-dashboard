import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery

# ──────────────────────────────────────────────
# ログイン認証
# ──────────────────────────────────────────────
from auth import require_login
require_login()

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
# 5. 月別推移グラフ（指標ごとに分けて表示・実績値表示付き）
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

    # 実績値ラベル（表示用）
    df_plot["実績値_label"] = df_plot["実績値"].apply(
        lambda x: f"¥{x:,.0f}" if 指標 in ["CPA", "CPC", "CPM"] else f"{x:.1%}"
    )

    import plotly.graph_objects as go
    fig = go.Figure()

    # 実績値の線
    fig.add_trace(go.Scatter(
        x=df_plot["配信月_dt"],
        y=df_plot["実績値"],
        mode="lines+markers+text",
        name="実績値",
        text=df_plot["実績値_label"],
        textposition="top center",
        line=dict(color="blue")
    ))

    # 目標値の線
    fig.add_trace(go.Scatter(
        x=df_plot["配信月_dt"],
        y=df_plot["目標値"],
        mode="lines+markers",
        name="目標値",
        line=dict(color="gray", dash="dash")
    ))

    fig.update_layout(
        yaxis_title=指標,
        xaxis_title="配信月",
        xaxis_tickformat="%Y/%m",
        height=400,
        hovermode="x unified"
    )
    st.plotly_chart(fig, use_container_width=True)



# ------------------------------------------------------------
# 6. 配信月 × カテゴリ 複合折れ線グラフ（指標別タブ）
# ------------------------------------------------------------
st.markdown("### 📈 配信月 × カテゴリ 複合折れ線グラフ（指標別）")

指標リスト = ["CPA", "CVR", "CPC", "CPM"]
折れ線タブ = st.tabs(指標リスト)

for 指標, tab in zip(指標リスト, 折れ線タブ):
    with tab:
        st.markdown(f"#### 📉 {指標} 達成率の推移（カテゴリ別）")

        best_col = f"{指標}_best"
        rate_col = f"{指標}_達成率"

        # データフィルタと達成率計算
        df_line = df[df[best_col].notna() & df[指標].notna()].copy()
        df_line[rate_col] = df_line[指標] / df_line[best_col]
        df_line["配信月_str"] = df_line["配信月_dt"].dt.strftime("%Y/%m")

        # 月×カテゴリごとの平均達成率を集計
        df_grouped_line = (
            df_line.groupby(["配信月_str", "カテゴリ"])
                   .agg(達成率平均=(rate_col, "mean"))
                   .reset_index()
        )

        # グラフ描画
        import plotly.express as px
        fig = px.line(
            df_grouped_line,
            x="配信月_str",
            y="達成率平均",
            color="カテゴリ",
            markers=True,
            labels={"配信月_str": "配信月", "達成率平均": f"{指標}達成率"}
        )
        fig.update_layout(
            yaxis_tickformat=".0%",
            xaxis_title="配信月",
            yaxis_title=f"{指標}達成率",
            height=500
        )
        st.plotly_chart(fig, use_container_width=True)



# ------------------------------------------------------------
# 7. 達成率バーグラフ（都道府県別・タブ切り替え）
# ------------------------------------------------------------
st.markdown("### 📊 都道府県別 達成率バーグラフ（指標別）")

指標リスト = ["CPA", "CVR", "CPC", "CPM"]
タブ = st.tabs(指標リスト)

for 指標, tab in zip(指標リスト, タブ):
    with tab:
        st.markdown(f"#### 🧭 都道府県別 {指標} 達成率")

        # 対象データ抽出
        col_best = f"{指標}_best"
        df_metric = df[df[col_best].notna() & df[指標].notna()].copy()
        df_metric[f"{指標}_達成率"] = df_metric[指標] / df_metric[col_best]

        # 都道府県別集計
        df_grouped = (
            df_metric.groupby("都道府県")
                     .agg(達成率平均=(f"{指標}_達成率", "mean"))
                     .reset_index()
        )

        # 評価分類
        def get_label(rate):
            if rate >= 1.0:
                return "良好"
            elif rate >= 0.9:
                return "注意"
            else:
                return "低調"

        df_grouped["評価"] = df_grouped["達成率平均"].apply(get_label)
        df_grouped["達成率（％）"] = df_grouped["達成率平均"].apply(lambda x: f"{x:.0%}")

        # 色マッピング（落ち着いた色）
        color_map = {
            "良好": "#B8E0D2",  # 薄緑
            "注意": "#FFF3B0",  # 薄黄
            "低調": "#F4C2C2"   # 薄赤
        }
        df_grouped["色"] = df_grouped["評価"].map(color_map)

        # グラフ用データ並び替え
        df_sorted = df_grouped.sort_values("達成率平均", ascending=True)

        # 棒グラフ描画（横型）
        import plotly.graph_objects as go
        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=df_sorted["達成率平均"],
            y=df_sorted["都道府県"],
            orientation="h",
            text=df_sorted["達成率（％）"],
            textposition="outside",
            marker_color=df_sorted["色"],
            hovertemplate="%{y}<br>達成率：%{text}<extra></extra>"
        ))

        fig.update_layout(
            height=400,  # ✅ 縦幅を半分に
            xaxis=dict(title=f"{指標}達成率", tickformat=".0%"),
            yaxis=dict(title="都道府県"),
            margin=dict(l=100, r=40, t=40, b=40)
        )

        st.plotly_chart(fig, use_container_width=True)



