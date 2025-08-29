# pages/08_🤖_Ad_Chatbot.py
# -*- coding: utf-8 -*-
"""
🤖 Ad Chatbot
- NOTION_JOINED_AD_DATA を参照し、自然言語での質問に回答
- GPT (OpenAI API) を利用して分析・会話を生成
- 画像URLは inline 表示、動画URLは埋め込み or リンク表示
"""

import streamlit as st
from google.cloud import bigquery
import google.auth
from google.auth import impersonated_credentials
import pandas as pd
import openai
import re

# ============ ページ設定 ============
st.set_page_config(page_title="🤖 Ad Chatbot", layout="wide")
st.title("🤖 Ad Chatbot")
st.caption("広告数値 × Notion情報を自然言語で会話形式に分析します。")

# ============ OpenAI 設定 ============
openai.api_key = st.secrets["openai"]["api_key"]

# ============ BQ クライアント生成（Impersonation） ============
def get_notion_client():
    base_creds, _ = google.auth.default()
    target_sa = "notion-ad-bq-access@shosan-ad-expertai.iam.gserviceaccount.com"
    impersonated_creds = impersonated_credentials.Credentials(
        source_credentials=base_creds,
        target_principal=target_sa,
        target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
        lifetime=300,
    )
    return bigquery.Client(credentials=impersonated_creds, project="shosan-ad-expertai")

# ============ チャット履歴管理 ============
if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": "こんにちは！広告データとNotionの情報を組み合わせて分析できます。何を知りたいですか？"}
    ]

user_input = st.chat_input("広告の効果について聞いてみましょう…")
if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})

# ============ 表示 ============
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"], unsafe_allow_html=True)

# ============ BigQuery Helper ============
def query_bigquery(sql: str) -> pd.DataFrame:
    client = get_notion_client()
    return client.query(sql).to_dataframe()

# ============ URL 検出 & 埋め込み表示 ============
def render_media_from_text(text: str):
    url_pattern = r"(https?://[^\s]+)"
    urls = re.findall(url_pattern, text)

    for url in urls:
        if any(ext in url.lower() for ext in [".jpg", ".jpeg", ".png", ".gif", "googleusercontent"]):
            st.image(url, use_column_width=True)
        elif "youtube.com" in url or "youtu.be" in url or url.lower().endswith(".mp4"):
            try:
                st.video(url)
            except Exception:
                st.markdown(f"[▶ 動画を見る]({url})")
        else:
            st.markdown(f"[リンク]({url})")

# ============ GPT 呼び出し ============
def run_chat(user_message: str):
    df = query_bigquery("""
        SELECT *
        FROM `shosan-ad-expertai.SHOSAN_Notion_Data.NOTION_JOINED_AD_DATA`
        ORDER BY AD_DELIVERY_MONTH DESC
        LIMIT 50
    """)

    system_prompt = """
    あなたは広告分析アシスタントです。
    以下のテーブルデータを参考に、ユーザーの質問に答えてください。
    - 数値はDataFrameから取得し、推測で作らないこと
    - 画像URLが含まれていれば回答文に出してください
    - 動画URLが含まれていれば回答文に出してください
    """

    data_str = df.head(10).to_csv(index=False)

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"質問: {user_message}\n\n参考データ:\n{data_str}"}
    ]

    response_stream = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        stream=True
    )

    full_response = ""
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        for chunk in response_stream:
            delta = chunk.choices[0].delta.content or ""
            full_response += delta
            message_placeholder.markdown(full_response + "▌")
        message_placeholder.markdown(full_response)

        # --- URL埋め込み表示 ---
        render_media_from_text(full_response)

    st.session_state.messages.append({"role": "assistant", "content": full_response})

# ============ 入力があれば処理 ============
if user_input:
    run_chat(user_input)


















# # 03_🧠AI_Insight.py - 分析強化版　
# import streamlit as st
# import pandas as pd, numpy as np
# from google.cloud import bigquery
# import openai

# # ──────────────────────────────────────────────
# # ログイン認証
# # ──────────────────────────────────────────────
# from auth import require_login
# require_login()

# st.markdown("###### じゅんびちゅうです。")

# # openai.api_key = st.secrets["openai"]["api_key"]

# # st.set_page_config(page_title="広告AI分析室", layout="wide")
# # st.title("🧠 広告AI分析室")
# # st.subheader("🔍 パフォーマンス × クリエイティブ × AIによるインサイト")

# # # BigQuery認証 & データ取得
# # def load_data():
# #     info = dict(st.secrets["connections"]["bigquery"])
# #     info["private_key"] = info["private_key"].replace("\\n", "\n")
# #     client = bigquery.Client.from_service_account_info(info)
# #     df = client.query("""
# #         SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data
# #     """).to_dataframe()
# #     return df

# # df = load_data()

# # # フィルタ
# # st.markdown("<h5>📆 データ期間のフィルタ</h5>", unsafe_allow_html=True)
# # df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
# # dmin, dmax = df["Date"].min().date(), df["Date"].max().date()
# # sel = st.date_input("期間を選択", (dmin, dmax), min_value=dmin, max_value=dmax)
# # if isinstance(sel, (list, tuple)) and len(sel) == 2:
# #     s, e = map(pd.to_datetime, sel)
# #     df = df[(df["Date"].dt.date >= s.date()) & (df["Date"].dt.date <= e.date())]

# # # 数値変換
# # df["Cost"] = pd.to_numeric(df["Cost"], errors="coerce").fillna(0)
# # df["Clicks"] = pd.to_numeric(df["Clicks"], errors="coerce").fillna(0)
# # df["Impressions"] = pd.to_numeric(df["Impressions"], errors="coerce").fillna(0)
# # df["コンバージョン数"] = pd.to_numeric(df["コンバージョン数"], errors="coerce").fillna(0)

# # # 最新行抽出
# # latest_df = (
# #     df.sort_values("Date")
# #       .dropna(subset=["Date"])
# #       .loc[lambda d: d.groupby("CampaignId")["Date"].idxmax()]
# # )

# # # 指標計算
# # latest_df["CTR"] = latest_df["Clicks"] / latest_df["Impressions"].replace(0, np.nan)
# # latest_df["CVR"] = latest_df["コンバージョン数"] / latest_df["Clicks"].replace(0, np.nan)
# # latest_df["CPA"] = latest_df["Cost"] / latest_df["コンバージョン数"].replace(0, np.nan)
# # latest_df["CPC"] = latest_df["Cost"] / latest_df["Clicks"].replace(0, np.nan)
# # latest_df["CPM"] = latest_df["Cost"] * 1000 / latest_df["Impressions"].replace(0, np.nan)

# # # 分析対象の項目（カテゴリ別集計 + コピー列 + 数値列）
# # copied_cols = [
# #     "HeadlineByAdType", "HeadlinePart1ByAdType", "HeadlinePart2ByAdType",
# #     "Description1ByAdType", "Description2ByAdType"
# # ]

# # group_summary = latest_df.groupby("カテゴリ")[["CTR", "CVR", "CPA", "CPC", "CPM"]].mean().sort_values("CVR", ascending=False)

# # # AIコメント生成関数
# # def generate_ai_comment():
# #     table = group_summary.to_string(float_format=lambda x: f"{x:.2f}")
# #     prompt = f"""
# # 以下は広告カテゴリごとの平均広告指標（CTR, CVR, CPA, CPC, CPM）です：

# # {table}

# # また、以下はそれぞれの広告に含まれるテキスト要素（コピー）です：

# # {latest_df[copied_cols].dropna().head(10).to_string(index=False)}

# # この情報を元に、以下の分析を日本語で行ってください：

# # ## 1. カテゴリ別の数値傾向
# # どのカテゴリが高い成果を出しており、それはなぜか？

# # ## 2. コピーライティングの傾向分析
# # 上位カテゴリに共通するコピー表現はあるか？

# # ## 3. 数値に基づく画像の特徴分析
# # 画像が成果に与える影響について仮説を述べる（CVR, CTR, CPC, CPMの指標から）

# # ## 4. 改善すべきカテゴリと改善提案
# # CVRやCTRが低いカテゴリにはどのような改善策が考えられるか？

# # ## 総合まとめ
# # 傾向と戦略全体をまとめてください。
# #     """
# #     response = openai.chat.completions.create(
# #         model="gpt-3.5-turbo",
# #         messages=[{"role": "user", "content": prompt}]
# #     )
# #     return response.choices[0].message.content.strip()

# # # ボタンで生成
# # if st.button("🤖 AIに分析してもらう"):
# #     with st.spinner("AIが分析中..."):
# #         try:
# #             st.markdown("### 💬 AIコメント:")
# #             st.info(generate_ai_comment())
# #         except Exception as e:
# #             st.error("コメント生成中にエラーが発生しました")
# #             st.exception(e)
