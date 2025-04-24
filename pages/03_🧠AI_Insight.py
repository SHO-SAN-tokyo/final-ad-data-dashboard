# 03_🧠AI_Insight.py
import streamlit as st
import pandas as pd, numpy as np, plotly.express as px
from google.cloud import bigquery
import openai

openai.api_key = st.secrets["openai"]["api_key"]

st.set_page_config(page_title="広告AI分析室", layout="wide")
st.title("🧠 広告AI分析室")
st.subheader("🔍 パフォーマンス × クリエイティブ × AIによるインサイト")

# 認証とデータ取得
def load_data():
    info = dict(st.secrets["connections"]["bigquery"])
    info["private_key"] = info["private_key"].replace("\\n", "\n")
    client = bigquery.Client.from_service_account_info(info)
    df = client.query("SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data").to_dataframe()
    return df

df = load_data()

# 日付フィルタ
st.markdown("<h5>📅 データ期間のフィルタ</h5>", unsafe_allow_html=True)
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
dmin, dmax = df["Date"].min().date(), df["Date"].max().date()
sel = st.date_input("期間を選択", (dmin, dmax), min_value=dmin, max_value=dmax)
if isinstance(sel, (list, tuple)) and len(sel) == 2:
    s, e = map(pd.to_datetime, sel)
    df = df[(df["Date"].dt.date >= s.date()) & (df["Date"].dt.date <= e.date())]

# 数値列変換
df["Cost"] = pd.to_numeric(df["Cost"], errors="coerce").fillna(0)
df["Clicks"] = pd.to_numeric(df["Clicks"], errors="coerce").fillna(0)
df["Impressions"] = pd.to_numeric(df["Impressions"], errors="coerce").fillna(0)
df["\u30b3\u30f3\u30d0\u30fc\u30b8\u30e7\u30f3\u6570"] = pd.to_numeric(df["\u30b3\u30f3\u30d0\u30fc\u30b8\u30e7\u30f3\u6570"], errors="coerce").fillna(0)

# 最新行を抽出
latest_df = (
    df.sort_values("Date")
      .dropna(subset=["Date"])
      .loc[lambda d: d.groupby("CampaignId")["Date"].idxmax()]
)

# 指標計算
latest_df["CTR"] = latest_df["Clicks"] / latest_df["Impressions"].replace(0, np.nan)
latest_df["CVR"] = latest_df["\u30b3\u30f3\u30d0\u30fc\u30b8\u30e7\u30f3\u6570"] / latest_df["Clicks"].replace(0, np.nan)
latest_df["CPA"] = latest_df["Cost"] / latest_df["\u30b3\u30f3\u30d0\u30fc\u30b8\u30e7\u30f3\u6570"].replace(0, np.nan)

# 🔝 CVR上位/下位 表示
st.markdown("<h5>📸 CVR 上位 / 下位 広告ギャラリー</h5>", unsafe_allow_html=True)
cvr_ranked = latest_df.dropna(subset=["CVR", "CloudStorageUrl"]).sort_values("CVR", ascending=False)

tabs = st.tabs(["🏆 CVR上位", "🔻 CVR下位"])
with tabs[0]:
    top_ads = cvr_ranked.head(5)
    for _, row in top_ads.iterrows():
        st.image(row["CloudStorageUrl"], caption=f"{row['AdName']}\nCVR: {row['CVR']*100:.2f}%")

with tabs[1]:
    bottom_ads = cvr_ranked.tail(5)
    for _, row in bottom_ads.iterrows():
        st.image(row["CloudStorageUrl"], caption=f"{row['AdName']}\nCVR: {row['CVR']*100:.2f}%")

# 🤖 ChatGPTによる分析

def generate_ai_comment(df: pd.DataFrame):
    summary = df.groupby("カテゴリ")["CVR"].mean().sort_values(ascending=False).to_string()
    prompt = f"""
    以下は広告のカテゴリ別CVR平均値です:\n{summary}
    このデータをもとに、以下を日本語で分析・出力してください:
    - CVRが高いカテゴリの働向
    - コピーや画像の特徴で成功パターンが見えるか？
    - 改善すべきカテゴリにはどんな工夫が必要か？
    - 広告戦略の提案
    簡潔に、やや親しみあるトーンで。
    """
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content.strip()

if st.button("🤖 AIに分析してもらう"):
    with st.spinner("AIが分析中..."):
        try:
            st.markdown("### 💬 AIコメント:")
            st.info(generate_ai_comment(latest_df))
        except Exception as e:
            st.error("コメント生成中にエラーが発生しました")
            st.exception(e)  # ← この1行を追加
