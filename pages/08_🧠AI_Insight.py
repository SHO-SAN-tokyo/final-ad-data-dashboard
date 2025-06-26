# 03_🧠AI_Insight.py - 分析強化版　
import streamlit as st
import pandas as pd, numpy as np
from google.cloud import bigquery
import openai

# ──────────────────────────────────────────────
# ログイン認証
# ──────────────────────────────────────────────
from auth import require_login
require_login()

st.markdown("###### じゅんびちゅうです。")

# openai.api_key = st.secrets["openai"]["api_key"]

# st.set_page_config(page_title="広告AI分析室", layout="wide")
# st.title("🧠 広告AI分析室")
# st.subheader("🔍 パフォーマンス × クリエイティブ × AIによるインサイト")

# # BigQuery認証 & データ取得
# def load_data():
#     info = dict(st.secrets["connections"]["bigquery"])
#     info["private_key"] = info["private_key"].replace("\\n", "\n")
#     client = bigquery.Client.from_service_account_info(info)
#     df = client.query("""
#         SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data
#     """).to_dataframe()
#     return df

# df = load_data()

# # フィルタ
# st.markdown("<h5>📆 データ期間のフィルタ</h5>", unsafe_allow_html=True)
# df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
# dmin, dmax = df["Date"].min().date(), df["Date"].max().date()
# sel = st.date_input("期間を選択", (dmin, dmax), min_value=dmin, max_value=dmax)
# if isinstance(sel, (list, tuple)) and len(sel) == 2:
#     s, e = map(pd.to_datetime, sel)
#     df = df[(df["Date"].dt.date >= s.date()) & (df["Date"].dt.date <= e.date())]

# # 数値変換
# df["Cost"] = pd.to_numeric(df["Cost"], errors="coerce").fillna(0)
# df["Clicks"] = pd.to_numeric(df["Clicks"], errors="coerce").fillna(0)
# df["Impressions"] = pd.to_numeric(df["Impressions"], errors="coerce").fillna(0)
# df["コンバージョン数"] = pd.to_numeric(df["コンバージョン数"], errors="coerce").fillna(0)

# # 最新行抽出
# latest_df = (
#     df.sort_values("Date")
#       .dropna(subset=["Date"])
#       .loc[lambda d: d.groupby("CampaignId")["Date"].idxmax()]
# )

# # 指標計算
# latest_df["CTR"] = latest_df["Clicks"] / latest_df["Impressions"].replace(0, np.nan)
# latest_df["CVR"] = latest_df["コンバージョン数"] / latest_df["Clicks"].replace(0, np.nan)
# latest_df["CPA"] = latest_df["Cost"] / latest_df["コンバージョン数"].replace(0, np.nan)
# latest_df["CPC"] = latest_df["Cost"] / latest_df["Clicks"].replace(0, np.nan)
# latest_df["CPM"] = latest_df["Cost"] * 1000 / latest_df["Impressions"].replace(0, np.nan)

# # 分析対象の項目（カテゴリ別集計 + コピー列 + 数値列）
# copied_cols = [
#     "HeadlineByAdType", "HeadlinePart1ByAdType", "HeadlinePart2ByAdType",
#     "Description1ByAdType", "Description2ByAdType"
# ]

# group_summary = latest_df.groupby("カテゴリ")[["CTR", "CVR", "CPA", "CPC", "CPM"]].mean().sort_values("CVR", ascending=False)

# # AIコメント生成関数
# def generate_ai_comment():
#     table = group_summary.to_string(float_format=lambda x: f"{x:.2f}")
#     prompt = f"""
# 以下は広告カテゴリごとの平均広告指標（CTR, CVR, CPA, CPC, CPM）です：

# {table}

# また、以下はそれぞれの広告に含まれるテキスト要素（コピー）です：

# {latest_df[copied_cols].dropna().head(10).to_string(index=False)}

# この情報を元に、以下の分析を日本語で行ってください：

# ## 1. カテゴリ別の数値傾向
# どのカテゴリが高い成果を出しており、それはなぜか？

# ## 2. コピーライティングの傾向分析
# 上位カテゴリに共通するコピー表現はあるか？

# ## 3. 数値に基づく画像の特徴分析
# 画像が成果に与える影響について仮説を述べる（CVR, CTR, CPC, CPMの指標から）

# ## 4. 改善すべきカテゴリと改善提案
# CVRやCTRが低いカテゴリにはどのような改善策が考えられるか？

# ## 総合まとめ
# 傾向と戦略全体をまとめてください。
#     """
#     response = openai.chat.completions.create(
#         model="gpt-3.5-turbo",
#         messages=[{"role": "user", "content": prompt}]
#     )
#     return response.choices[0].message.content.strip()

# # ボタンで生成
# if st.button("🤖 AIに分析してもらう"):
#     with st.spinner("AIが分析中..."):
#         try:
#             st.markdown("### 💬 AIコメント:")
#             st.info(generate_ai_comment())
#         except Exception as e:
#             st.error("コメント生成中にエラーが発生しました")
#             st.exception(e)
