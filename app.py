import streamlit as st

st.set_page_config(page_title="HOME", layout="wide")
st.title("🏠 HOME - ダッシュボード入口")

st.markdown("### 🔗 ページ一覧")

def section(page_key, label, description):
    st.markdown(f"""
    <div style="margin-bottom:1.2rem;">
      <a href="/{page_key}" target="_self" style="text-decoration: none; font-weight: bold; font-size: 1.1rem;">{label}</a><br>
      <span style="color:#666;">{description}</span>
    </div>
    """, unsafe_allow_html=True)

section("01_🐬Ad_Drive", "🐬 Ad Drive", "バナー単位で広告の数値・CVを表示。絞り込みや並び替えに対応。")
section("02_🧩Unit_Score", "🧩 Unit Score", "各担当者ユニットごとのキャンペーン成果とスコアを表示。")
section("03_🔶Client_Page", "🔶 Client Page", "クライアント別に広告成果を確認。CVリストや広告内容も表示。")
section("04_📂My_Page", "📂 My Page", "ログインユーザー専用ページ。関連キャンペーンを表示。")
section("05_📈Report", "📈 Report", "月別の広告指標や傾向をレポート形式で表示。")
section("06_🧬SHO-SAN_market", "🧬 SHO-SAN market", "全体の市場データを分析し、AIが指標を評価・コメント。")
section("07_🎨LP_Score", "🎨 LP Score", "ランディングページの成果や改善状況を可視化。")
section("08_🧠AI_Insight", "🧠 AI Insight", "AIが広告パフォーマンスを多角的に分析し、改善提案。")
section("09_⚙️Unit_Settings", "⚙️ Unit設定", "ユニット（担当者グループ）情報の設定・編集。")
section("10_⚙️Client_Settings", "⚙️ Client設定", "クライアント情報や広告設定の管理。")
section("11_⚙️KPI_Settings", "⚙️ KPI設定", "カテゴリ・広告目的ごとの目標指標を登録・編集。")
