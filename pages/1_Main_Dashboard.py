import streamlit as st
from google.cloud import bigquery
import pandas as pd
import re

# ------------------------------------------------------------
# (省略) ページ設定とデータ取得のコード
# ------------------------------------------------------------

# ---------- 前処理とフィルタリングのコード (省略) ----------

# ------------------------------------------------------------
# 2. 画像バナー表示
# ------------------------------------------------------------
st.subheader("🌟 並び替え")
img_df = df[df["CloudStorageUrl"].astype(str).str.startswith("http")].copy()

# (省略) img_df の初期処理と重複削除のコード

def get_cv(r):
    n = r["AdNum"]
    if pd.isna(n): return 0
    col = str(int(n))
    return r[col] if col in r and isinstance(r[col], (int, float)) else 0
img_df["CV件数_base"] = img_df.apply(get_cv, axis=1) # 元のCV件数を保持

latest_text_map = {}
if "Date" in img_df.columns:
    latest = (img_df.sort_values("Date")
                .dropna(subset=["Date"])
                .loc[lambda d: d.groupby("AdName")["Date"].idxmax()])
    latest_text_map = latest.set_index("AdName")["Description1ByAdType"].to_dict()
else:
    st.warning("⚠️ 'Date' 列が img_df に存在しないため、メインテキストの取得をスキップします。")

agg_df = df.copy()
agg_df["AdName"] = agg_df["AdName"].astype(str).str.strip()
agg_df["CampaignId"] = agg_df["CampaignId"].astype(str).str.strip()
agg_df["AdNum"] = pd.to_numeric(agg_df["AdName"], errors="coerce")
agg_df = agg_df[agg_df["AdNum"].notna()]
agg_df["AdNum"] = agg_df["AdNum"].astype(int)

cv_sum_df = img_df.groupby(["CampaignId", "AdName"])["CV件数_base"].sum().reset_index() # 元のCV件数を使用

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

# デバッグ: img_df の結合キーの値をすべて表示
st.subheader("デバッグ: img_df の結合キーの値 (すべて)")
st.dataframe(img_df[["CampaignId", "AdName"]])

# デバッグ: caption_df の結合キーの値をすべて表示
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

sort_opt = st.radio("並び替え基準", ["広告番号順", "コンバージョン数の多い順", "CPAの低い順"])
if sort_opt == "コンバージョン数の多い順":
    img_df = img_df[img_df["CV件数"] > 0].sort_values("CV件数", ascending=False)
elif sort_opt == "CPAの低い順":
    img_df = img_df[img_df["CPA"].notna()].sort_values("CPA")
else:
    img_df = img_df.sort_values("AdNum")

st.subheader("デバッグ: 並び替え後 img_df")
st.write(f"img_df の行数 (並び替え後): {len(img_df)}")
st.dataframe(img_df)

cols = st.columns(5, gap="small")

def parse_canva_links(raw: str) -> list[str]:
    parts = re.split(r'[,\s]+', str(raw or ""))
    return [p for p in parts if p.startswith("http")]

for idx, (_, row) in enumerate(img_df.iterrows()):
    ad  = row["AdName"]
    cid = row["CampaignId"]
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
