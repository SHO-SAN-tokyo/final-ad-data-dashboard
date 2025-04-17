# 1_Main_Dashboard.py
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import re                             # ← 追加（canvaURL 抽出用）

# ------------------------------------------------------------
# 0. ページ設定 & “画像カード” 用の軽い CSS だけ追加
#    ＊データ取得やフィルタのロジックには一切触れていません
# ------------------------------------------------------------
st.set_page_config(page_title="配信バナー", layout="wide")

st.markdown(
    """
    <style>
      .banner-card{
        padding:12px 12px 20px 12px;border:1px solid #e6e6e6;border-radius:12px;
        background:#fafafa;height:100%;margin-bottom:14px;
      }
      .banner-card img{
        width:100%;height:180px;object-fit:cover;border-radius:8px;
        cursor:pointer;
      }
      .banner-caption{margin-top:8px;font-size:14px;line-height:1.6;text-align:left;}
      .gray-text{color:#888;}
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🖼️ 配信バナー")

# ------------------------------------------------------------
# 1. 認証 & データ取得（ロジック変更なし）
# ------------------------------------------------------------
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

query = "SELECT * FROM careful-chess-406412.SHOSAN_Ad_Tokyo.Final_Ad_Data"
with st.spinner("🔄 データを取得中..."):
    df = client.query(query).to_dataframe()
# ← 取得が終わるとアニメーションもメッセージも自動で消える

try:
    df = client.query(query).to_dataframe()

    if df.empty:
        st.warning("⚠️ データがありません")
        st.stop()
    else:
        st.success("✅ データ取得成功！")

        # ---------- 前処理 ----------
        if "カテゴリ" in df.columns:
            df["カテゴリ"] = df["カテゴリ"].astype(str).str.strip().replace("", "未設定").fillna("未設定")
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

        # ---------- 日付フィルタ ----------
        if "Date" in df.columns and not df["Date"].isnull().all():
            min_d, max_d = df["Date"].min().date(), df["Date"].max().date()
            sel_date = st.sidebar.date_input("日付フィルター", (min_d, max_d),
                                             min_value=min_d, max_value=max_d)
            if isinstance(sel_date, (list, tuple)) and len(sel_date) == 2:
                s, e = map(pd.to_datetime, sel_date)
                date_filtered_df = df[(df["Date"].dt.date >= s.date()) &
                                      (df["Date"].dt.date <= e.date())]
            else:
                one = pd.to_datetime(sel_date).date()
                date_filtered_df = df[df["Date"].dt.date == one]
        else:
            date_filtered_df = df.copy()

        st.sidebar.header("🔍 フィルター")

        # ---------- クライアント検索 ----------
        all_clients = sorted(date_filtered_df["PromotionName"].dropna().unique())

        def update_client():
            cs = st.session_state.client_search
            if cs in all_clients:
                st.session_state.selected_client = cs

        st.sidebar.text_input("クライアント検索", "", key="client_search",
                              placeholder="Enter で決定", on_change=update_client)

        search_val = st.session_state.get("client_search", "")
        filtered_clients = [c for c in all_clients if search_val.lower() in c.lower()] \
                           if search_val else all_clients
        c_opts = ["すべて"] + filtered_clients
        sel_client = st.sidebar.selectbox(
            "クライアント", c_opts,
            index=c_opts.index(st.session_state.get("selected_client", "すべて"))
        )
        client_filtered_df = (date_filtered_df[date_filtered_df["PromotionName"] == sel_client]
                              if sel_client != "すべて" else date_filtered_df.copy())

        # ---------- カテゴリ / キャンペーン ----------
        sel_cat = st.sidebar.selectbox("カテゴリ", ["すべて"] + sorted(client_filtered_df["カテゴリ"].dropna().unique()))
        client_cat_filtered_df = (client_filtered_df[client_filtered_df["カテゴリ"] == sel_cat]
                                  if sel_cat != "すべて" else client_filtered_df.copy())

        sel_cmp = st.sidebar.selectbox("キャンペーン名", ["すべて"] + sorted(client_cat_filtered_df["CampaignName"].dropna().unique()))
        filtered_df = (client_cat_filtered_df[client_cat_filtered_df["CampaignName"] == sel_cmp]
                       if sel_cmp != "すべて" else client_cat_filtered_df.copy())

        # ---------- 表形式 ----------
        # st.subheader("📋 表形式データ")
        # st.dataframe(filtered_df)

        # ---------- 1〜60 列補完 ----------
        for i in range(1, 61):
            col = str(i)
            filtered_df[col] = pd.to_numeric(filtered_df.get(col, 0), errors="coerce").fillna(0)

        # ------------------------------------------------------------
        # 2. 画像バナー表示
        # ------------------------------------------------------------
        st.subheader("🌟並び替え")
        if "CloudStorageUrl" in filtered_df.columns:
            with st.spinner("🔄 画像を取得中..."):
                img_df = filtered_df[filtered_df["CloudStorageUrl"]
                                      .astype(str).str.startswith("http")].copy()
                # 以降のロジックはそのまま

            img_df = filtered_df[filtered_df["CloudStorageUrl"].astype(str)
                                 .str.startswith("http")].copy()
            img_df["AdName"] = img_df["AdName"].astype(str).str.strip()
            img_df["CampaignId"] = img_df["CampaignId"].astype(str).str.strip()
            img_df["CloudStorageUrl"] = img_df["CloudStorageUrl"].astype(str).str.strip()
            img_df["AdNum"] = pd.to_numeric(img_df["AdName"], errors="coerce")
            img_df = img_df.drop_duplicates(subset=["CampaignId", "AdName", "CloudStorageUrl"])

            for col in ["Cost", "Impressions", "Clicks"]:
                if col in filtered_df.columns:
                    filtered_df[col] = pd.to_numeric(filtered_df[col], errors="coerce")

            def get_cv(r):
                n = r["AdNum"]
                if pd.isna(n):
                    return 0
                col = str(int(n))
                return r[col] if col in r and isinstance(r[col], (int, float)) else 0

            img_df["CV件数"] = img_df.apply(get_cv, axis=1)

            latest = img_df.sort_values("Date").dropna(subset=["Date"])
            latest = latest.loc[latest.groupby("AdName")["Date"].idxmax()]
            latest_text_map = latest.set_index("AdName")["Description1ByAdType"].to_dict()

            agg_df = filtered_df.copy()
            agg_df["AdName"] = agg_df["AdName"].astype(str).str.strip()
            agg_df["CampaignId"] = agg_df["CampaignId"].astype(str).str.strip()
            agg_df["AdNum"] = pd.to_numeric(agg_df["AdName"], errors="coerce")
            agg_df = agg_df[agg_df["AdNum"].notna()]
            agg_df["AdNum"] = agg_df["AdNum"].astype(int)

            cv_sum_df = img_df.groupby(["CampaignId", "AdName"])["CV件数"].sum().reset_index()

            caption_df = (agg_df.groupby(["CampaignId", "AdName"])
                          .agg({"Cost": "sum", "Impressions": "sum", "Clicks": "sum"})
                          .reset_index()
                          .merge(cv_sum_df, on=["CampaignId", "AdName"], how="left"))
            caption_df["CTR"] = caption_df["Clicks"] / caption_df["Impressions"]
            caption_df["CPA"] = caption_df.apply(
                lambda r: (r["Cost"] / r["CV件数"]) if pd.notna(r["CV件数"]) and r["CV件数"] > 0 else pd.NA, axis=1
            )
            caption_map = caption_df.set_index(["CampaignId", "AdName"]).to_dict("index")

            sort_opt = st.radio("並び替え基準", ["広告番号順", "コンバージョン数の多い順", "CPAの低い順"])
            if sort_opt == "コンバージョン数の多い順":
                img_df = img_df[img_df["CV件数"] > 0].sort_values("CV件数", ascending=False)
            elif sort_opt == "CPAの低い順":
                img_df = img_df[img_df["CPA"].notna()].sort_values("CPA")
            else:
                img_df = img_df.sort_values("AdNum")

            if img_df.empty:
                st.warning("⚠️ 表示できる画像がありません")
            else:
                cols = st.columns(5, gap="small")

                def parse_canva_links(raw: str) -> list[str]:
                    """http(s) から始まる URL を抽出して返す"""
                    if not raw:
                        return []
                    # カンマ・改行・スペースで分割し http(s) で始まる物だけ
                    parts = re.split(r'[,\s]+', str(raw))
                    return [p for p in parts if p.startswith("http")]

                for idx, (_, row) in enumerate(img_df.iterrows()):
                    ad  = row["AdName"]
                    cid = row["CampaignId"]
                    v   = caption_map.get((cid, ad), {})
                    cost, imp, clicks = v.get("Cost", 0), v.get("Impressions", 0), v.get("Clicks", 0)
                    ctr, cpa, cv = v.get("CTR"), v.get("CPA"), v.get("CV件数", 0)
                    text = latest_text_map.get(ad, "")

                    # ---- canvaURL 解析 ----
                    canva_raw = row.get("canvaURL", "")
                    links = parse_canva_links(canva_raw)
                    if links:
                        if len(links) == 1:
                            canva_html = f'<a href="{links[0]}" target="_blank" rel="noopener">canvaURL↗️</a>'
                        else:
                            canva_html = ", ".join(
                                f'<a href="{l}" target="_blank" rel="noopener">canvaURL{i+1}↗️</a>'
                                for i, l in enumerate(links)
                            )
                    else:
                        canva_html = '<span class="gray-text">canvaURL：なし✖</span>'

                    cap_html = f"""
                      <div class='banner-caption'>
                        <b>広告名：</b>{ad}<br>
                        <b>消化金額：</b>{cost:,.0f}円<br>
                        <b>IMP：</b>{imp:,.0f}<br>
                        <b>クリック：</b>{clicks:,.0f}<br>
                        <b>CTR：</b>{ctr*100:.2f}%<br>""" if pd.notna(ctr) else """
                        <b>CTR：</b>-<br>
                      """
                    cap_html += f"<b>CV数：</b>{int(cv) if cv > 0 else 'なし'}<br>"
                    cap_html += f"<b>CPA：</b>{cpa:,.0f}円<br>" if pd.notna(cpa) else "<b>CPA：</b>-<br>"
                    cap_html += f"<b></b>{canva_html}<br>"
                    cap_html += f"<b>メインテキスト：</b>{text}</div>"

                    # ---- 画像をクリックで拡大（別タブ） ----
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

except Exception as e:
    st.error(f"❌ データ取得エラー: {e}")
