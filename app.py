import streamlit as st

st.set_page_config(page_title="HOME", layout="wide")
st.title("🏠 HOME - ダッシュボード入口")
st.markdown("### 🔗 ページ一覧")

def section(label, description, path):
    st.markdown(f"**{label}**")
    st.markdown(f"<span style='color:#666;'>{description}</span>", unsafe_allow_html=True)
    st.page_link(path, label="▶ ページを開く")
    st.markdown("---")

section("🐬 Ad Drive", "バナー単位の広告パフォーマンスを画像とともに一覧表示", "pages/01_🐬Ad_Drive.py")
section("🔷 Unit Score", "Unit単位でのKPI達成状況を一覧で確認", "pages/02_🔷Unit_Score.py")
section("🔶 Client Page", "クライアント別に広告の効果や指標を確認", "pages/03_🔶Client_Page.py")
section("📂 My Page", "担当者自身の配信・成果・ログなどを確認", "pages/04_📂My_Page.py")
section("📈 Report", "月別の広告指標や傾向をレポート形式で表示", "pages/05_📈Report.py")
section("🧩 SHO-SAN market", "カテゴリ・目的別に市場全体の傾向を分析", "pages/06_🧩SHO-SAN_market.py")
section("🎨 LP Score", "ランディングページの品質をAIが評価・提案", "pages/07_🎨LP_Score.py")
section("🧠 AI Insight", "AIによる改善コメントやインサイトを自動生成", "pages/08_🧠AI_Insight.py")
section("⚙️ Unit Settings", "Unitや担当者の管理・設定", "pages/09_⚙️Unit_Settings.py")
section("⚙️ Client Settings", "クライアントの基本情報やIDを管理", "pages/10_⚙️Client_Settings.py")
section("⚙️ KPI Settings", "広告のカテゴリ・目的ごとのKPI目標値を設定", "pages/11_⚙️KPI_Settings.py")
