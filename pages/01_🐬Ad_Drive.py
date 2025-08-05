import streamlit as st
import pandas as pd
from google.cloud import bigquery
import re
import html

# ──────────────────────────────────────────────
# ログイン認証
# ──────────────────────────────────────────────
from auth import require_login
require_login()

# ──────────────────────────────────────────────
# ページ設定 & BigQuery 認証
# ──────────────────────────────────────────────
st.set_page_config(page_title="🐬 Ad Drive", layout="wide")

# --- タイトルとボタンを横並びで表示 ---
col1, col2 = st.columns([6, 1])  # 左を広く
with col1:
    st.markdown("<h1 style='display:inline-block;margin-bottom:0;'>🐬 Ad Drive ／すべてのクライアント</h1>", unsafe_allow_html=True)
with col2:
    # 右端にボタン
    btn_style = """
    <style>
    div[data-testid="column"]:nth-of-type(2) button {
        float: right !important;
        margin-top: 8px;
        margin-right: 6px;
    }
    </style>
    """
    st.markdown(btn_style, unsafe_allow_html=True)
    if st.button("🔄 キャッシュクリア", key="refresh_btn"):
        st.cache_data.clear()
        st.rerun()

cred = dict(st.secrets["connections"]["bigquery"])
cred["private_key"] = cred["private_key"].replace("\\n", "\n")
bq = bigquery.Client.from_service_account_info(cred)

# ──────────────────────────────────────────────
# ① データ取得
#    キャンペーン指標   … Final_Ad_Data_Last
#    バナー指標         … Banner_Drive_Ready
# ──────────────────────────────────────────────
df_num = bq.query(
    "SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data_Last`"
).to_dataframe()

df_banner = bq.query(
    "SELECT * FROM `careful-chess-406412.SHOSAN_Ad_Tokyo.Banner_Drive_Ready`"
).to_dataframe()

if df_num.empty and df_banner.empty:
    st.warning("データが存在しません")
    st.stop()

# ──────────────────────────────────────────────
# ② 前処理／列リネーム（※CV 列を分離）
# ──────────────────────────────────────────────
rename_common = {
    "媒体": "ServiceNameJA",
    "クライアント名": "client_name"
}

df_num = df_num.rename(columns={
    **rename_common,
    "コンバージョン数": "conv_total"      # キャンペーン総 CV
})
df_banner = df_banner.rename(columns={
    **rename_common,
    "CV": "conv_banner"                  # バナー別 CV
})

# 数値型を明示
for col in ("conv_total", "conv_banner"):
    if col in df_num.columns:
        df_num[col] = pd.to_numeric(df_num[col], errors="coerce")
    if col in df_banner.columns:
        df_banner[col] = pd.to_numeric(df_banner[col], errors="coerce")

# 配信月は “YYYY/MM” 文字列
for d in (df_num, df_banner):
    if "配信月" in d.columns:
        d["配信月"] = d["配信月"].astype(str)

# ──────────────────────────────────────────────
# ③ フィルター UI  ※df_num 基準でマスタ値を取得
# ──────────────────────────────────────────────
st.markdown("<h3 class='top'>🔎 広告を絞り込む</h3>", unsafe_allow_html=True)

# 最初は全データ
filtered = df_num.copy()

# --- 1段目: 配信月 & クライアント名 ---
col1, col2 = st.columns(2)
with col1:
    month_options = sorted(filtered["配信月"].dropna().unique())
    sel_month = st.multiselect("📅 配信月", month_options, placeholder="すべて")
    if sel_month:
        filtered = filtered[filtered["配信月"].isin(sel_month)]
with col2:
    client_options = sorted(filtered["client_name"].dropna().unique())
    sel_client = st.multiselect("👤 クライアント名", client_options, placeholder="すべて")
    if sel_client:
        filtered = filtered[filtered["client_name"].isin(sel_client)]

# --- 2段目: メインカテゴリ・サブカテゴリ・広告媒体・広告目的（横並び） ---
col3, col4, col5, col6, col7 = st.columns(5)

with col3:
    media_options = sorted(filtered["広告媒体"].dropna().unique())
    sel_media = st.multiselect("📡 広告媒体", media_options, placeholder="すべて")
    if sel_media:
        filtered = filtered[filtered["広告媒体"].isin(sel_media)]

with col4:
    cat_options = sorted(filtered["メインカテゴリ"].dropna().unique())
    sel_cat = st.multiselect("📁 メインカテゴリ", cat_options, placeholder="すべて")
    if sel_cat:
        filtered = filtered[filtered["メインカテゴリ"].isin(sel_cat)]

with col5:
    subcat_options = sorted(filtered["サブカテゴリ"].dropna().unique())
    sel_subcat = st.multiselect("📂 サブカテゴリ", subcat_options, placeholder="すべて")
    if sel_subcat:
        filtered = filtered[filtered["サブカテゴリ"].isin(sel_subcat)]

with col6:
    specialcat_options = sorted(filtered["特殊カテゴリ"].dropna().unique())
    sel_specialcat = st.multiselect("🏷️ 特殊カテゴリ", specialcat_options, placeholder="すべて")
    if sel_specialcat:
        filtered = filtered[filtered["特殊カテゴリ"].isin(sel_specialcat)]

with col7:
    goal_options = sorted(filtered["広告目的"].dropna().unique())
    sel_goal = st.multiselect("🎯 広告目的", goal_options, placeholder="すべて")
    if sel_goal:
        filtered = filtered[filtered["広告目的"].isin(sel_goal)]

# --- キャンペーン名・広告セット名（横並び） ---
camp_col, adg_col = st.columns(2)

with camp_col:
    camp_options = sorted(filtered["キャンペーン名"].dropna().unique())
    sel_campaign = st.multiselect("📣 キャンペーン名", camp_options, placeholder="すべて")
    if sel_campaign:
        filtered = filtered[filtered["キャンペーン名"].isin(sel_campaign)]

with adg_col:
    adg_options = sorted(filtered["広告セット名"].dropna().unique())
    sel_adgroup = st.multiselect("*️⃣ 広告セット名", adg_options, placeholder="すべて")
    if sel_adgroup:
        filtered = filtered[filtered["広告セット名"].isin(sel_adgroup)]

# --- キーワード検索（広告セット名のみ部分一致、複数ワードOK） ---
keyword = st.text_input(
    "🔍 広告セット名キーワード検索（複数ワードはカンマ区切り可）",
    value="",
    placeholder="例: 動画,静止画,30秒"
)




# ──────────────────────────────────────────────
# ④ フィルター関数（キャンペーン / バナー両方へ適用）
# ──────────────────────────────────────────────
def apply_filters(
    df: pd.DataFrame,
    sel_client=None, sel_month=None,
    sel_cat=None, sel_subcat=None,
    sel_goal=None, sel_media=None,
    sel_specialcat=None,
    sel_campaign=None,
    sel_adgroup=None,
    keyword=None,
    search_in_camp=True,
    search_in_adg=True
) -> pd.DataFrame:
    cond = pd.Series(True, index=df.index)
    if "client_name" in df.columns and sel_client:
        cond &= df["client_name"].isin(sel_client)
    if "配信月" in df.columns and sel_month:
        cond &= df["配信月"].isin(sel_month)
    if "広告媒体" in df.columns and sel_media:
        cond &= df["広告媒体"].isin(sel_media)
    if "メインカテゴリ" in df.columns and sel_cat:
        cond &= df["メインカテゴリ"].isin(sel_cat)
    if "サブカテゴリ" in df.columns and sel_subcat:
        cond &= df["サブカテゴリ"].isin(sel_subcat)
    if "特殊カテゴリ" in df.columns and sel_specialcat:
        cond &= df["特殊カテゴリ"].isin(sel_specialcat)
    if "広告目的" in df.columns and sel_goal:
        cond &= df["広告目的"].isin(sel_goal)
    if "キャンペーン名" in df.columns and sel_campaign:
        cond &= df["キャンペーン名"].isin(sel_campaign)
    if "広告セット名" in df.columns and sel_adgroup:
        cond &= df["広告セット名"].isin(sel_adgroup)

    # ▼▼▼ ここが追加部分！ ▼▼▼
    if keyword and (search_in_camp or search_in_adg):
        keywords = [w.strip() for w in keyword.split(",") if w.strip()]
        if keywords:
            subcond = pd.Series(False, index=df.index)
            if search_in_camp and "キャンペーン名" in df.columns:
                subcond = subcond | df["キャンペーン名"].astype(str).apply(
                    lambda x: any(kw.lower() in x.lower() for kw in keywords)
                )
            if search_in_adg and "広告セット名" in df.columns:
                subcond = subcond | df["広告セット名"].astype(str).apply(
                    lambda x: any(kw.lower() in x.lower() for kw in keywords)
                )
            cond &= subcond
    # ▲▲▲ ここまで ▲▲▲

    return df.loc[cond].copy()


df_num_filt = apply_filters(
    df_num,
    sel_client=sel_client,
    sel_month=sel_month,
    sel_cat=sel_cat,
    sel_subcat=sel_subcat,
    sel_goal=sel_goal,
    sel_media=sel_media,
    sel_specialcat=sel_specialcat,
    sel_campaign=sel_campaign,
    sel_adgroup=sel_adgroup,
    keyword=keyword,
    search_in_camp=search_in_camp,
    search_in_adg=search_in_adg,
)

df_banner_filt = apply_filters(
    df_banner,
    sel_client=sel_client,
    sel_month=sel_month,
    sel_cat=sel_cat,
    sel_subcat=sel_subcat,
    sel_goal=sel_goal,
    sel_media=sel_media,
    sel_specialcat=sel_specialcat,
    sel_campaign=sel_campaign,
    sel_adgroup=sel_adgroup,
    keyword=keyword,
    search_in_camp=search_in_camp,
    search_in_adg=search_in_adg,
)


# バナーは画像 URL がある行だけを軽量表示 (最大 100 件)
df_banner_disp = df_banner_filt[df_banner_filt["CloudStorageUrl"].notna()].head(100) if "CloudStorageUrl" in df_banner_filt.columns else df_banner_filt.head(100)

# ──────────────────────────────────────────────
# ⑤ KPI スコアカード（キャンペーン単位）
# ──────────────────────────────────────────────
total_cost  = df_num_filt["Cost"].sum() if "Cost" in df_num_filt.columns else 0
total_click = df_num_filt["Clicks"].sum() if "Clicks" in df_num_filt.columns else 0
total_cv    = df_num_filt["conv_total"].sum() if "conv_total" in df_num_filt.columns else 0
total_imp   = df_num_filt["Impressions"].sum() if "Impressions" in df_num_filt.columns else 0

cpa = total_cost / total_cv if total_cv else None
cvr = total_cv / total_click if total_click else None
ctr = total_click / total_imp if total_imp else None
cpm = (total_cost * 1000 / total_imp) if total_imp else None
cpc = total_cost / total_click if total_click else None

# 配信月：空やNaN対策
if "配信月" not in df_num_filt.columns or df_num_filt["配信月"].dropna().empty:
    delivery_range = "-"
else:
    delivery_range = f"{df_num_filt['配信月'].dropna().min()} 〜 {df_num_filt['配信月'].dropna().max()}"

st.markdown(
    f"📅 配信月：{delivery_range}　"
    f"👤 クライアント：{sel_client or 'すべて'}<br>"
    f"📁 メインカテゴリ：{sel_cat or 'すべて'}　"
    f"📂 サブカテゴリ：{sel_subcat or 'すべて'}　"
    f"🏷️ 特殊カテゴリ：{sel_specialcat or 'すべて'}"
    f"📡 広告媒体：{sel_media or 'すべて'}　"
    f"🎯 広告目的：{sel_goal or 'すべて'}<br>"
    f"📣 キャンペーン名：{sel_campaign or 'すべて'}<br>"
    f"*️⃣ 広告セット名：{sel_adgroup or 'すべて'}",
    unsafe_allow_html=True
)

#キャッシュクリアボタン
cols = st.columns([5,1])
with cols[1]:
    if st.button("🔄️データ更新"):
        st.cache_data.clear()
        st.rerun()

# ──────────────────────────────────────────────
# ⑥ 広告数値
# ──────────────────────────────────────────────
st.subheader("💠 広告数値")
# 3列（上段）
row1 = [
    {"label": "CPA - 獲得単価", "value": f"{cpa:,.0f}円" if cpa else "-", "bg": "#fff"},
    {"label": "コンバージョン数", "value": f"{int(total_cv):,}", "bg": "#fff"},
    {"label": "CVR - コンバージョン率", "value": f"{cvr*100:,.2f}%" if cvr else "-", "bg": "#fff"},
    {"label": "消化金額", "value": f"{total_cost:,.0f}円", "bg": "#fff"},
]
cols1 = st.columns(4)
for i, card in enumerate(row1):
    with cols1[i]:
        st.markdown(f"""
            <div class="scorecard" style="
                background:{card['bg']};
                border-radius: 11px;
                padding: .8rem;
                margin-bottom: 0.8rem;
                box-shadow: 0 2px 6px rgba(50,60,80,.04);
                border:1px solid #e4e4e4;">
              <div class="scorecard-label" style="font-size:12px; color:#111; margin-bottom:2px;">
                {card['label']}
              </div>
              <div class="scorecard-value" style="font-size:1.35rem; font-weight:600; color:#111; letter-spacing:0.01em; margin-bottom: 0.8rem !important;">
                {card['value']}
              </div>
            </div>
        """, unsafe_allow_html=True)

# 5列（下段）
row2 = [
    {"label": "インプレッション", "value": f"{int(total_imp):,}", "bg": "#fff"},
    {"label": "CTR - クリック率", "value": f"{ctr*100:,.2f}%" if ctr else "-", "bg": "#fff"},
    {"label": "CPC - クリック単価",   "value": f"{cpc:,.0f}円" if cpc else "-",     "bg": "#fff"},
    {"label": "CPM", "value": f"{cpm:,.0f}" if cpm else "-", "bg": "#fff"},
    {"label": "クリック", "value": f"{int(total_click):,}", "bg": "#fff"},
]
cols2 = st.columns(5)
for i, card in enumerate(row2):
    with cols2[i]:
        st.markdown(f"""
            <div class="scorecard" style="
                background:{card['bg']};
                border-radius: 11px;
                padding: .8rem;
                margin-bottom: 0.8rem;
                box-shadow: 0 2px 6px rgba(50,60,80,.04);
                border:1px solid #e4e4e4;">
              <div class="scorecard-label" style="font-size:12px; color:#111; margin-bottom:2px;">
                {card['label']}
              </div>
              <div class="scorecard-value" style="font-size:1.35rem; font-weight:600; color:#111; letter-spacing:0.01em; margin-bottom: 0.8rem !important;">
                {card['value']}
              </div>
            </div>
        """, unsafe_allow_html=True)

# ──────────────────────────────────────────────
# ⑥-A キャンペーン一覧表 UI
# ──────────────────────────────────────────────
# --- キャンペーン単位（キーワード・広告セット名で絞り込まないDFで集計）
df_num_campaign_only = apply_filters(
    df_num,
    sel_client=sel_client,
    sel_month=sel_month,
    sel_cat=sel_cat,
    sel_subcat=sel_subcat,
    sel_goal=sel_goal,
    sel_media=sel_media,
    sel_specialcat=sel_specialcat,
    sel_campaign=sel_campaign,
    sel_adgroup=None,       # ★広告セット名フィルター無効
    keyword=None,           # ★キーワード検索も無効
    search_in_camp=False,
    search_in_adg=False,
)

# --- キャンペーン単位集計表 ---
st.markdown("""
<div style="background:#ddedfc;padding:.6rem 1.2rem;margin:2rem 0 1rem 0;font-size:2.1rem;font-weight:700;letter-spacing:.04em;">
  <img src="https://img.icons8.com/color/48/000000/combo-chart--v1.png" style="width:38px;vertical-align:-8px;margin-right:8px;">キャンペーン単位 集計
</div>
""", unsafe_allow_html=True)

# --- デバッグ表示 ---
with st.expander("🦚 デバッグ（キャンペーン＋配信月ごとの件数）", expanded=False):
    debug_count = df_num_campaign_only.groupby(["キャンペーン名", "配信月"]).size().reset_index(name="件数")
    st.dataframe(debug_count, use_container_width=True, hide_index=True)

if not df_num_campaign_only.empty:
    camp_grouped = (
        df_num_campaign_only.groupby(["キャンペーン名", "配信月"], as_index=False)
        .agg({
            "Cost": "sum",
            "conv_total": "sum",
            "Impressions": "sum",
            "Clicks": "sum"
        })
    )
    camp_grouped["CPA"] = camp_grouped["Cost"] / camp_grouped["conv_total"]
    camp_grouped["CTR"] = camp_grouped["Clicks"] / camp_grouped["Impressions"]
    camp_grouped["CVR"] = camp_grouped["conv_total"] / camp_grouped["Clicks"]

    # フォーマット
    camp_grouped["Cost"] = camp_grouped["Cost"].map(lambda x: f"¥{x:,.0f}" if pd.notna(x) else "-")
    camp_grouped["CPA"] = camp_grouped["CPA"].map(lambda x: f"¥{x:,.0f}" if pd.notna(x) else "-")
    camp_grouped["CTR"] = camp_grouped["CTR"].map(lambda x: f"{x*100:.2f}%" if pd.notna(x) else "-")
    camp_grouped["CVR"] = camp_grouped["CVR"].map(lambda x: f"{x*100:.2f}%" if pd.notna(x) else "-")
    camp_grouped["Impressions"] = camp_grouped["Impressions"].map(lambda x: f"{int(x):,}" if pd.notna(x) else "-")
    camp_grouped["Clicks"] = camp_grouped["Clicks"].map(lambda x: f"{int(x):,}" if pd.notna(x) else "-")
    camp_grouped["conv_total"] = camp_grouped["conv_total"].map(lambda x: f"{int(x):,}" if pd.notna(x) else "-")

    show_cols = [
        "キャンペーン名", "配信月", "Cost", "conv_total", "CPA", "Impressions", "Clicks", "CTR", "CVR"
    ]
    st.dataframe(camp_grouped[show_cols].head(1000), use_container_width=True, hide_index=True)
else:
    st.info("データがありません")



# --- キャンペーン＋広告セット単位集計表 ---
st.markdown("### 📑 キャンペーン＋広告セット単位 集計")
if not df_num_filt.empty:
    camp_adg_grouped = (
        df_num_filt.groupby(["キャンペーン名", "広告セット名", "配信月"], as_index=False)
        .agg({
            "Cost": "sum",
            "conv_total": "sum",
            "Impressions": "sum",
            "Clicks": "sum"
        })
    )
    camp_adg_grouped["CPA"] = camp_adg_grouped["Cost"] / camp_adg_grouped["conv_total"]
    camp_adg_grouped["CTR"] = camp_adg_grouped["Clicks"] / camp_adg_grouped["Impressions"]
    camp_adg_grouped["CVR"] = camp_adg_grouped["conv_total"] / camp_adg_grouped["Clicks"]

    # フォーマット
    camp_adg_grouped["Cost"] = camp_adg_grouped["Cost"].map(lambda x: f"¥{x:,.0f}" if pd.notna(x) else "-")
    camp_adg_grouped["CPA"] = camp_adg_grouped["CPA"].map(lambda x: f"¥{x:,.0f}" if pd.notna(x) else "-")
    camp_adg_grouped["CTR"] = camp_adg_grouped["CTR"].map(lambda x: f"{x*100:.2f}%" if pd.notna(x) else "-")
    camp_adg_grouped["CVR"] = camp_adg_grouped["CVR"].map(lambda x: f"{x*100:.2f}%" if pd.notna(x) else "-")
    camp_adg_grouped["Impressions"] = camp_adg_grouped["Impressions"].map(lambda x: f"{int(x):,}" if pd.notna(x) else "-")
    camp_adg_grouped["Clicks"] = camp_adg_grouped["Clicks"].map(lambda x: f"{int(x):,}" if pd.notna(x) else "-")
    camp_adg_grouped["conv_total"] = camp_adg_grouped["conv_total"].map(lambda x: f"{int(x):,}" if pd.notna(x) else "-")

    show_cols2 = [
        "キャンペーン名", "広告セット名", "配信月", "Cost", "conv_total", "CPA", "Impressions", "Clicks", "CTR", "CVR"
    ]
    st.dataframe(camp_adg_grouped[show_cols2].head(1000), use_container_width=True, hide_index=True)
else:
    st.info("データがありません")


# ──────────────────────────────────────────────
# ⑦ バナー並び替え UI
# ──────────────────────────────────────────────
st.subheader("💠 配信バナー")
st.write("###### ※一度に表示できる配信バナーの表示は最大100件です")
order = st.radio("🐬並び替え基準", ["広告番号順", "コンバージョン数の多い順", "CPA金額の安い順"])

df_banner_sorted = df_banner_filt.copy()
if order == "コンバージョン数の多い順":
    df_banner_sorted = df_banner_sorted.sort_values("conv_banner", ascending=False)
elif order == "CPA金額の安い順":
    df_banner_sorted = df_banner_sorted[df_banner_sorted["CPA"].notna()].sort_values("CPA")
elif order == "広告番号順":
    if "banner_number" in df_banner_sorted.columns:
        df_banner_sorted = df_banner_sorted.copy()
        df_banner_sorted["banner_number"] = pd.to_numeric(df_banner_sorted["banner_number"], errors="coerce")
        df_banner_sorted = df_banner_sorted.sort_values("banner_number", na_position="last")
    else:
        st.warning("⚠️ banner_number列が存在しません。元の順序で表示します。")

# 「ソートした後で」CloudStorageUrlのある上位100件のみ表示
df_banner_disp = df_banner_sorted[df_banner_sorted["CloudStorageUrl"].notna()].head(100)



# ──────────────────────────────────────────────
# ⑧ バナーカード描画
# ──────────────────────────────────────────────
def split_urls(raw):
    urls = re.split(r"[,\s　]+", str(raw or ""))
    urls = [u.strip() for u in urls if u.strip().startswith("http")]
    return urls

cols = st.columns(3, gap="small")
for i, (_, row) in enumerate(df_banner_disp.iterrows()):
    cost = row.get("Cost", 0)
    imp  = row.get("Impressions", 0)
    clk  = row.get("Clicks", 0)
    cv   = int(row.get("conv_banner", 0)) if pd.notna(row.get("conv_banner", 0)) else 0
    cpa_ = row.get("CPA")
    ctr_ = row.get("CTR")
    cpc_ = row.get("CPC") if "CPC" in row and pd.notna(row.get("CPC")) else (cost / clk if clk else None)

    canva_links = split_urls(row.get("canvaURL", ""))
    if canva_links:
        canva_html = "<br>".join(
            f'<a href="{html.escape(u)}" target="_blank">canvaURL{"↗️" if j == 0 else str(j+1)+"↗️"}</a>'
            for j, u in enumerate(canva_links)
        )
    else:
        canva_html = '<span class="gray-text">canvaURL：未記入</span>'

    # --- LP /遷移先 URL ---
    url_links = split_urls(row.get("URL", ""))
    if url_links:
        url_html = "<br>".join(
            f'<a href="{html.escape(u)}" target="_blank">飛び先URL{"↗️" if j == 0 else str(j+1)+"↗️"}</a>'
            for j, u in enumerate(url_links)
        )
    else:
        url_html = '<span class="gray-text">飛び先URL：未記入</span>'


    caption = [
        f"<div style='font-size:9px;color:#888;margin-bottom:-17px;line-height:1.4;'>{row.get('キャンペーン名','')}</div>",
        f"<b>広告名：</b>{row.get('AdName', '')}",
        f"<b>消化金額：</b>{cost:,.0f}円",
        f"<b>IMP：</b>{imp:,.0f}",
        f"<b>クリック：</b>{clk:,.0f}",
        f"<b>CTR：</b>{ctr_*100:.2f}%" if pd.notna(ctr_) else "<b>CTR：</b>-",
        f"<b>CPC：</b>{cpc_:,.0f}円" if cpc_ is not None else "<b>CPC：</b>-",
        f"<b>CV数：</b>{cv if cv else 'なし'}",
        f"<b>CPA：</b>{cpa_:,.0f}円" if pd.notna(cpa_) else "<b>CPA：</b>-",
        url_html,
        canva_html,
        f"<b>メインテキスト：</b>{row.get('Description', '')}"
    ]

    card_html = f"""
      <div class='banner-card'>
        <a href="{row.get('CloudStorageUrl', '')}" target="_blank" rel="noopener">
          <img src="{row.get('CloudStorageUrl', '')}">
        </a>
        <div class='banner-caption'>{"<br>".join(caption)}</div>
      </div>
    """
    with cols[i % 3]:
        st.markdown(card_html, unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  フォント & CSS
# ──────────────────────────────────────────────
st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;700&display=swap" rel="stylesheet">
""", unsafe_allow_html=True)

theme = st.get_option("theme.base")
is_dark = theme == "dark"

st.markdown("""
<style>
  h3.top {
    margin: .4rem auto 1rem auto !important;
    font-size: 1.5rem;
  }
  h3 {
    background-color: #ddedfc;
    padding: .6rem !important;
    display: block;
    margin: 2rem auto 1rem auto !important;
  }
  .scorecard-label {
    font-size: 14px;
    margin-bottom: 6px;
    text-align: left;
    padding: .3rem .6rem;
  }

  .scorecard-value {
    font-size: 30px;
    text-align: left;
    line-height: 1.2;
    font-weight: 600;
    padding: 2px 10px;
    margin-bottom: 1.4rem;
    font-family: 'Inter', 'Roboto', sans-serif;
  }

  .banner-card {
    padding: 12px 12px 20px;
    border: 1px solid #e6e6e6;
    border-radius: 12px;
    background: #fafafa;
    height: auto;
    margin-bottom: 14px;
  }

  .banner-card img {
    width: 100%;
    height: auto;
    object-fit: contain;
    border-radius: 8px;
    cursor: pointer;
  }

  .banner-caption {
    margin-top: 8px;
    font-size: 12px;
    line-height: 1.6;
    text-align: left;
    color: #111;
    font-family: 'Inter', 'Roboto', sans-serif;
  }

  .gray-text {
    color: #888;
  }
  
  .st-emotion-cache-16tyu1 p {
    margin-bottom: 2rem;
  }
</style>
""", unsafe_allow_html=True)
