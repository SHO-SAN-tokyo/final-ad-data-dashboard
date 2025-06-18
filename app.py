import streamlit as st

st.set_page_config(page_title="HOME", layout="wide")
st.title("🏠 HOME")
st.markdown("### 🔗 ページ一覧")

st.page_link("pages/01_🐬Ad_Drive.py", label="🐬 Ad Drive", help="バナー単位の広告パフォーマンス一覧と画像表示")
st.page_link("pages/02_🔷Unit_Score.py", label="🔷 Unit Score", help="Unit単位でのKPI分析・達成率の確認")
st.page_link("pages/03_🔶Client_Page.py", label="🔶 Client Page", help="クライアント別に指標とバナーを一覧で確認")
st.page_link("pages/04_📂My_Page.py", label="📂 My Page", help="担当者別マイページ（ログやCV数を含む）")
st.page_link("pages/05_📈Report.py", label="📈 Report", help="月次レポートや全体指標の確認")
st.page_link("pages/06_🧩SHO-SAN_market.py", label="🧩 SHO-SAN market", help="カテゴリ・目的別の広告市場のKPI分析")
st.page_link("pages/07_🎨LP_Score.py", label="🎨 LP Score", help="ランディングページの評価や改善点をAIが分析")
st.page_link("pages/08_🧠AI_Insight.py", label="🧠 AI Insight", help="広告データからAIによる改善提案を表示")
st.page_link("pages/09_⚙️Unit_Settings.py", label="⚙️ Unit Settings", help="Unitや担当者の設定管理ページ")
st.page_link("pages/10_⚙️Client_Settings.py", label="⚙️ Client Settings", help="クライアント情報・ID管理")
st.page_link("pages/11_⚙️KPI_Settings.py", label="⚙️ KPI Settings", help="広告目的やカテゴリごとのKPI目標値を設定")
