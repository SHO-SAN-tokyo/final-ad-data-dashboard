import streamlit as st

st.set_page_config(page_title="HOME", layout="wide")
st.title("🏠 HOME")

st.markdown("### 🔗 ページ一覧")

def section(page_key, label, description):
    st.markdown(f"""
    <div style="margin-bottom:1.2rem;">
      <a href="/{page_key}" target="_self" style="text-decoration: none; font-weight: bold; font-size: 1.1rem;">{label}</a><br>
      <span style="color:#666;">{description}</span>
    </div>
    """, unsafe_allow_html=True)

section("Ad_Drive", "🐬 Ad Drive", "全クライアントの広告スコア／配信月別")
section("Unit_Score", "🔷 Unit Score", "各担当者ユニットごとの広告スコア・チーム達成率・個人達成率／配信月別")
section("Client_Page", "🔶 Client Page", "クライアント別に広告成果を確認。CVリストや広告内容も表示／日別・配信月別")
section("My_Page", "📂 My Page", "準備中です。運用者・フロントごとの広告スコア専用マイページ")
section("SHO-SAN_market", "🧩 SHO-SAN market", "準備中です。全体CPA・CTR・CPMなどの指標の推移、各カテゴリ・地方・都道府県別の広告スコア")
section("LP_Score", "🎨 LP Score", "LP・HPなど広告配信したURLごとに広告スコア／累計")
section("AI_Insight", "🧠 AI Insight", "準備中です。AIが広告パフォーマンスを多角的に分析し、改善提案。")
section("Unit_Settings", "⚙️ Unit設定", "ユニット情報の設定・編集。")
section("Client_Settings", "⚙️ Client設定", "クライアント情報情報の設定・編集。")
section("KPI_Settings", "⚙️ KPI設定", "カテゴリ・広告目的ごとの目標指標を登録・編集。")
