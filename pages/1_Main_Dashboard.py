import streamlit as st
from google.cloud import bigquery
import pandas as pd
import re

# ------------------------------------------------------------
# (省略) ページ設定 & CSS、データ取得、前処理、フィルタリング
# ------------------------------------------------------------

# ------------------------------------------------------------
# 2. 画像バナー表示
# ------------------------------------------------------------
st.subheader("🌟 並び替え")
img_df = df[df["CloudStorageUrl"].astype(str).str.startswith("http")].copy()

# (省略) img_df の初期処理、重複削除、CV件数_base の計算

# デバッグ出力: AdName、AdNum、CV件数_base を確認
st.subheader("デバッグ: img_df の AdName と AdNum と CV件数_base")
st.dataframe(img_df[["AdName", "AdNum", "CV件数_base"]].head(10))

latest_text_map = {}
# (省略) latest_text_map の生成

agg_df = df.copy()
# (省略) agg_df の処理

cv_sum_df = img_df.groupby(["CampaignId", "AdName"])["CV件数_base"].sum().reset_index()

caption_df = (
    agg_df.groupby(["CampaignId", "AdName"])
    .agg({"Cost": "sum", "Impressions": "sum", "Clicks": "sum"})
    .reset_index()
    .merge(cv_sum_df, on=["CampaignId", "AdName"], how="left")
)
caption_df["CTR"] = caption_df["Clicks"] / caption_df["Impressions"]
caption_df["CPA"] = caption_df.apply(
    lambda r: (r["Cost"] / r["CV件数_base"]) if pd.notna(r["CV件数_base"]) and r["CV件数_base"] > 0 else pd.NA,
    axis=1,
)

# デバッグ: caption_df の内容をすべて表示
st.subheader("デバッグ: caption_df の内容 (すべて)")
st.dataframe(caption_df)

# デバッグ: img_df の結合キーの値 (すべて)
st.subheader("デバッグ: img_df の結合キーの値 (すべて)")
st.dataframe(img_df[["CampaignId", "AdName"]])

# デバッグ: caption_df の結合キーの値 (すべて)
st.subheader("デバッグ: caption_df の結合キーの値 (すべて)")
st.dataframe(caption_df[["CampaignId", "AdName"]])

# マージ前に結合キーの空白文字を除去 (念のため再度)
img_df["CampaignId"] = img_df["CampaignId"].str.strip()
img_df["AdName"] = img_df["AdName"].str.strip()
caption_df["CampaignId"] = caption_df["CampaignId"].str.strip()
caption_df["AdName"] = caption_df["AdName"].str.strip()

# CPA / CV件数 を付与
merged_img_df = img_df.merge(
    caption_df[["CampaignId", "AdName", "CV件数_base", "CPA"]],
    on=["CampaignId", "AdName"],
    how="left"
)

st.subheader("デバッグ: マージ後 merged_img_df")
st.write(f"merged_img_df の行数 (マージ後): {len(merged_img_df)}")
st.dataframe(merged_img_df)

# マージ結果を img_df に代入
img_df = merged_img_df

# デバッグ: CPA 列が存在するか確認し、存在しない場合は警告
if "CPA" not in img_df.columns:
    st.warning("⚠️ CPA 列が img_df に存在しません。")
    img_df["CPA"] = pd.NA

# デバッグ: CV件数_base 列が存在するか確認し、存在しない場合は警告
if "CV件数_base" not in img_df.columns:
    st.warning("⚠️ CV件数_base 列が img_df に存在しません。")
    img_df["CV件数_base"] = 0 # 0で埋める

img_df["CPA"]     = pd.to_numeric(img_df["CPA"], errors="coerce")
img_df["CV件数"] = pd.to_numeric(img_df["CV件数_base"], errors="coerce").fillna(0) # 並び替えと表示用に元のCV件数を使用

caption_map = caption_df.set_index(["CampaignId", "AdName"]).to_dict("index")

# ここにデバッグコードを追加
st.subheader("デバッグ: caption_map の最初のキー")
st.write(list(caption_map.keys())[:5])

for idx, (_, row) in enumerate(img_df.iterrows()):
    ad  = row["AdName"]
    cid = row["CampaignId"]
    st.write(f"デバッグ表示ループ: cid='{cid}', ad='{ad}', type(cid)={type(cid)}, type(ad)={type(ad)}")
    v   = caption_map.get((cid, ad), {})
    cost, imp, clicks = v.get("Cost", 0), v.get("Impressions", 0), v.get("Clicks", 0)
    ctr, cpa, cv = v.get("CTR"), v.get("CPA"), v.get("CV件数", 0)
    text = latest_text_map.get(ad, "")

    # canvaURL
    links = parse_canva_links(row.get("canvaURL", ""))
    if links:
        canva_html = ", ".join(
            f'<a href="{l}" target="_blank" rel="noopener">canvaURL{i+1 if len(links)>1 else ""}↗️</a>'
            for i, l in enumerate(links)
        )
    else:
        canva_html = '<span class="gray-text">canvaURL：なし✖</span>'

    # ---------- caption 文字列 ----------
    cap_html = "<div class='banner-caption'>"
    cap_html += f"<b>広告名：</b>{ad}<br>"
    cap_html += f"<b>消化金額：</b>{cost:,.0f}円<br>"
    cap_html += f"<b>IMP：</b>{imp:,.0f}<br>"
    cap_html += f"<b>クリック：</b>{clicks:,.0f}<br>"
    if pd.notna(ctr):
        cap_html += f"<b>CTR：</b>{ctr*100:.2f}%<br>"
    else:
        cap_html += "<b>CTR：</b>-<br>"
    cap_html += f"<b>CV数：</b>{int(cv) if cv > 0 else 'なし'}<br>"
    cap_html += f"<b>CPA：</b>{cpa:,.0f}円<br>" if pd.notna(cpa) else "<b>CPA：</b>-<br>"
    cap_html += f"{canva_html}<br>"
    cap_html += f"<b>メインテキスト：</b>{text}</div>"

    card_html = f"""
        <div class='banner-card'>
            <a href="{row['CloudStorageUrl']}" target="_blank" rel="noopener">
                <img src="{row['CloudStorageUrl']}">
            </a>
            {cap_html}
        </div>
    """

    with cols[idx % 5]:
        st.markdown(card_html, unsafe_allow_html=True)
