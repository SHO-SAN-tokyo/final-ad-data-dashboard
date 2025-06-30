import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery

# ─────────────────────────────
# ログイン認証
# ─────────────────────────────
from auth import require_login
require_login()

# ─────────────────────────────
# ページ設定
# ─────────────────────────────
st.set_page_config(page_title="カテゴリ×都道府県 達成率モニター", layout="wide")
st.title("🧩 SHO‑SAN market")
st.subheader("📊 カテゴリ × 都道府県 キャンペーン達成率モニター")

cred = dict(st.secrets["connections"]["bigquery"])
cred["private_key"] = cred["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(cred)

@st.cache_data(show_spinner=False)
def load_data():
    query = "SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Market_Monthly_Evaluated_View`"
    return client.query(query).to_dataframe()

@st.cache_data(show_spinner=False)
def load_kpi_settings():
    query = "SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Target_Indicators_Meta`"
    return client.query(query).to_dataframe()

df = load_data()
df_kpi = load_kpi_settings()

df["配信月_dt"] = pd.to_datetime(df["配信月"] + "-01", errors="coerce")
df["配信月"] = df["配信月_dt"].dt.strftime("%Y/%m")

# KPIは常に完全固定
kpi_row = df_kpi[
    (df_kpi["メインカテゴリ"] == "注文住宅･規格住宅") &
    (df_kpi["サブカテゴリ"] == "完成見学会") &
    (df_kpi["広告目的"] == "コンバージョン")
].iloc[0]

kpi_dict = {
    "CPA": kpi_row["CPA_good"],
    "CVR": kpi_row["CVR_good"],
    "CTR": kpi_row["CTR_good"],
    "CPC": kpi_row["CPC_good"],
    "CPM": kpi_row["CPM_good"],
}

# 1キャンペーン1行
df_disp = df.drop_duplicates(
    subset=["配信月", "キャンペーン名", "メインカテゴリ", "サブカテゴリ", "広告目的"]
)

# フィルター
def option_list(colname):
    vals = df_disp[colname].dropna()
    return vals.value_counts().index.tolist()

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    main_cat_opts = option_list("メインカテゴリ")
    main_cat = st.multiselect("📁 メインカテゴリ", main_cat_opts, default=[], placeholder="すべて")
with col2:
    sub_cat_opts = option_list("サブカテゴリ")
    sub_cat = st.multiselect("🗂️ サブカテゴリ", sub_cat_opts, default=[], placeholder="すべて")
with col3:
    area_opts = option_list("地方")
    area = st.multiselect("🌏 地方", area_opts, default=[], placeholder="すべて")
with col4:
    pref_opts = option_list("都道府県")
    pref = st.multiselect("🗾 都道府県", pref_opts, default=[], placeholder="すべて")
with col5:
    obj_opts = option_list("広告目的")
    obj = st.multiselect("🎯 広告目的", obj_opts, default=[], placeholder="すべて")

df_filtered = df_disp.copy()
if main_cat:
    df_filtered = df_filtered[df_filtered["メインカテゴリ"].isin(main_cat)]
if sub_cat:
    df_filtered = df_filtered[df_filtered["サブカテゴリ"].isin(sub_cat)]
if area:
    df_filtered = df_filtered[df_filtered["地方"].isin(area)]
if pref:
    df_filtered = df_filtered[df_filtered["都道府県"].isin(pref)]
if obj:
    df_filtered = df_filtered[df_filtered["広告目的"].isin(obj)]

# 表示整形用関数
def get_label(val, indicator, is_kpi=False):
    if pd.isna(val):
        return ""
    if indicator in ["CPA", "CPC", "CPM"]:
        return f"¥{val:,.0f}"
    elif indicator in ["CVR", "CTR"]:
        if is_kpi:
            return f"{val:.1f}%"
        else:
            return f"{val*100:.1f}%"
    else:
        return f"{val}"

st.markdown("### 📋 達成率一覧")
表示列 = [
    "配信月", "都道府県", "地方", "メインカテゴリ", "サブカテゴリ", "広告目的", "キャンペーン名",
    "CPA", "CPA_good", "CPA_評価",
    "CVR", "CVR_good", "CVR_評価",
    "CTR", "CTR_good", "CTR_評価",
    "CPC", "CPC_good", "CPC_評価",
    "CPM", "CPM_good", "CPM_評価",
    "目標CPA"
]
df_fmt = df_filtered[表示列].copy()

# 指標ごとに書式変更
for col in ["CPA", "CPA_good", "CPC", "CPC_good", "CPM", "CPM_good", "目標CPA"]:
    df_fmt[col] = df_fmt[col].apply(lambda x: get_label(x, col.split("_")[0]))
for col in ["CVR", "CVR_good", "CTR", "CTR_good"]:
    is_kpi = "good" in col
    df_fmt[col] = df_fmt[col].apply(lambda x: get_label(x, col.split("_")[0], is_kpi=is_kpi))

st.dataframe(
    df_fmt.sort_values(["配信月", "都道府県", "メインカテゴリ", "サブカテゴリ", "キャンペーン名"]),
    use_container_width=True, hide_index=True
)

# 5. 月別推移グラフ（指標ごとに分けて表示・実績値表示付き）
st.markdown("### 📈 月別推移グラフ（指標別）")
指標群 = ["CPA", "CVR", "CTR", "CPC", "CPM"]
for 指標 in 指標群:
    st.markdown(f"#### 📉 {指標} 推移")
    df_plot = (
        df_filtered.groupby("配信月_dt")
          .agg(実績値=(指標, "mean"))
          .reset_index()
    )
    df_plot["実績値_label"] = df_plot["実績値"].apply(lambda v: get_label(v, 指標))
    kpi_value = kpi_dict[指標]
    kpi_label = get_label(kpi_value, 指標, is_kpi=True)
    df_plot["目標値"] = kpi_value
    df_plot["目標値_label"] = kpi_label
    import plotly.graph_objects as go
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_plot["配信月_dt"],
        y=df_plot["実績値"],
        mode="lines+markers+text",
        name="実績値",
        text=df_plot["実績値_label"],
        textposition="top center",
        line=dict(color="blue"),
        hovertemplate="%{x|%Y/%m}<br>実績値：%{text}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=df_plot["配信月_dt"],
        y=df_plot["目標値"],
        mode="lines+markers+text",
        name="目標値",
        text=[kpi_label]*len(df_plot),
        textposition="top center",
        line=dict(color="gray", dash="dash"),
        hovertemplate="%{x|%Y/%m}<br>目標値：%{text}<extra></extra>",
    ))
    fig.update_layout(
        yaxis_title=指標,
        xaxis_title="配信月",
        xaxis_tickformat="%Y/%m",
        height=400,
        hovermode="x unified"
    )
    st.plotly_chart(fig, use_container_width=True)

# 6. 配信月 × メインカテゴリ × サブカテゴリ 複合折れ線グラフ（指標別タブ）
st.markdown("### 📈 配信月 × メインカテゴリ × サブカテゴリ 複合折れ線グラフ（指標別）")
指標リスト = ["CPA", "CVR", "CPC", "CPM"]
折れ線タブ = st.tabs(指標リスト)
for 指標, tab in zip(指標リスト, 折れ線タブ):
    with tab:
        st.markdown(f"#### 📉 {指標} 達成率の推移（メインカテゴリ・サブカテゴリ別）")
        good_col = f"{指標}_good"
        rate_col = f"{指標}_達成率"
        df_line = df_filtered[df_filtered[good_col].notna() & df_filtered[指標].notna()].copy()
        df_line[rate_col] = df_line[指標] / df_line[good_col]
        df_line["配信月_str"] = df_line["配信月_dt"].dt.strftime("%Y/%m")
        # 月×メインカテゴリ×サブカテゴリごとの平均達成率
        df_grouped_line = (
            df_line.groupby(["配信月_str", "メインカテゴリ", "サブカテゴリ"])
                   .agg(達成率平均=(rate_col, "mean"))
                   .reset_index()
        )
        fig = px.line(
            df_grouped_line,
            x="配信月_str",
            y="達成率平均",
            color="メインカテゴリ",
            line_dash="サブカテゴリ",
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

# 7. 達成率バーグラフ（都道府県別・タブ切り替え）
st.markdown("### 📊 都道府県別 達成率バーグラフ（指標別）")
タブ = st.tabs(指標リスト)
for 指標, tab in zip(指標リスト, タブ):
    with tab:
        st.markdown(f"#### 🧭 都道府県別 {指標} 達成率")
        good_col = f"{指標}_good"
        df_metric = df_filtered[df_filtered[good_col].notna() & df_filtered[指標].notna()].copy()
        df_metric[f"{指標}_達成率"] = df_metric[指標] / df_metric[good_col]
        # 都道府県別集計
        df_grouped = (
            df_metric.groupby("都道府県")
                     .agg(達成率平均=(f"{指標}_達成率", "mean"))
                     .reset_index()
        )
        def get_label_bar(rate):
            if rate >= 1.0:
                return "良好"
            elif rate >= 0.9:
                return "注意"
            else:
                return "低調"
        df_grouped["評価"] = df_grouped["達成率平均"].apply(get_label_bar)
        df_grouped["達成率（％）"] = df_grouped["達成率平均"].apply(lambda x: f"{x:.0%}")
        color_map = {"良好": "#B8E0D2", "注意": "#FFF3B0", "低調": "#F4C2C2"}
        df_grouped["色"] = df_grouped["評価"].map(color_map)
        df_sorted = df_grouped.sort_values("達成率平均", ascending=True)
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
            height=400,
            xaxis=dict(title=f"{指標}達成率", tickformat=".0%"),
            yaxis=dict(title="都道府県"),
            margin=dict(l=100, r=40, t=40, b=40)
        )
        st.plotly_chart(fig, use_container_width=True)
