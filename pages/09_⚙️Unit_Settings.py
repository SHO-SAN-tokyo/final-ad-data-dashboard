import streamlit as st
from google.cloud import bigquery
import pandas as pd
from datetime import datetime

# ──────────────────────────────────────────────
# ログイン認証
# ──────────────────────────────────────────────
from auth import require_login
require_login()

# ──────────────────────────────────────────────
# Unit設定
# ──────────────────────────────────────────────
st.set_page_config(page_title="Unit設定", layout="wide")
st.title("⚙️ Unit設定")

# --- BigQuery接続 ---
info_dict = dict(st.secrets["connections"]["bigquery"])
info_dict["private_key"] = info_dict["private_key"].replace("\\n", "\n")
client = bigquery.Client.from_service_account_info(info_dict)

# --- テーブル定義 ---
project_id = "careful-chess-406412"
dataset = "SHOSAN_Ad_Tokyo"
table = "UnitMapping"
full_table = f"{project_id}.{dataset}.{table}"

# --- 担当者一覧の動的取得 ---
@st.cache_data(ttl=60)
def get_unique_tantousha():
    query = f"""
    SELECT DISTINCT `担当者`
    FROM `{project_id}.{dataset}.Final_Ad_Data`
    WHERE `担当者` IS NOT NULL AND `担当者` != ''
    ORDER BY `担当者`
    """
    return client.query(query).to_dataframe()

# --- Unit Mapping の取得 ---
@st.cache_data(ttl=60)
def load_unit_mapping():
    return client.query(f"SELECT * FROM {full_table}").to_dataframe()

def save_to_bq(df):
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("担当者", "STRING"),
            bigquery.SchemaField("所属", "STRING"),
            bigquery.SchemaField("雇用形態", "STRING"),
            bigquery.SchemaField("operator_id", "STRING"),
            bigquery.SchemaField("start_month", "STRING"),
            bigquery.SchemaField("end_month", "STRING"),
        ]
    )
    job = client.load_table_from_dataframe(df, full_table, job_config=job_config)
    job.result()

# --- データロード  ---
all_tantousha_df = get_unique_tantousha()
current_df = load_unit_mapping()

# 🔰 使い方マニュアル
with st.expander("📘 Unit設定 使い方マニュアル", expanded=False):
    st.markdown("""
このページでは、**広告運用メンバーの所属Unit管理**を行います。  
「誰がどのユニットに所属しているか」や「マイページ」「異動の履歴」などろまとめて確認・管理できます。

---

### 🌟 操作の流れ

#### ① 担当者を新しく追加したいとき
「➕ 担当者をUnitに追加」から以下を入力してください：
- 担当者名
- 所属するUnit名（例：Unit A）
- 雇用形態（例：社員、インターン）
- 所属を開始した月（例：2024-06）

✅ 入力後、「＋ 担当者を追加」ボタンを押すと登録されます。

---

#### ② 担当者が他のUnitに異動したとき
①🔁 Unit異動」のフォームに以下を入力してください：
1. 異動させたい担当者を選ぶ  
2. 新しい所属Unit名を入力（例：UnitA）※大文字小文字正しく入力  
3. 異動月を入力 ⇒ その担当者が新しいUnitに所属を開始する月（例：2025-08）
✅ 「異動を登録」ボタンを押すと、過去の所属が終了し、新しい所属として記録されます。

②保存後、少しスクロールして「:memo: 一覧編集（直接修正可）」
　・異動する担当者名を探す
　・異動する前の所属Unit名がある行を探す
　・終了月が空欄になってるので、前Unitを離脱した年月を入力（例：2025-07）

 ⚠️前Unitの終了月と、次Unitの開始月は同じでは（被っては）いけません⚠️
---

#### ③ 登録済みの情報を修正・削除したいとき
「📝 一覧編集（直接修正可）」の表で、直接入力して修正できます：

- 所属や開始月・終了月を直す
- 間違えて登録してしまった場合は、行を削除することで履歴を消すことも可能
- 編集後は「💾 修正内容を保存」ボタンを忘れずに押してください

---

#### ④ 所属状況や履歴を確認したいとき

- **🏷️ Unitごとの現在の担当者一覧**：今、どのUnitに誰がいるかを確認できます
- **📜 異動履歴**：これまでの異動の記録を一覧で見られます
- **🚪 メンバー外一覧**：過去に所属があったが、今はどこにも所属していない人を表示します

---

#### ⑤ 担当者が退職・もしくは広告TMから外れたとき

担当者が会社を辞めた場合は、**現在の所属行に「終了月（end_month）」を入力するだけでOKです。**  
新しい行の追加は不要です。

【手順】  
1. 「📝 一覧編集（直接修正可）」から該当の行を探す  
2. `end_month` 列に退職月を `YYYY-MM` 形式で入力（例：2024-10）  
3. 「💾 修正内容を保存」を押して反映完了

📌 `end_month` が入力されると、その担当者は「🚪 メンバー外一覧」に表示され、現在のUnitからは外れます。

---

### 🧠 よくある質問

- **Q. 間違えて消してしまったら？**  
　→ もう一度同じ情報を登録すれば元に戻せます。

- **Q. 間違って入力してしまったら？**  
　→ 「📝 一覧編集」で修正してください。

- **Q. 異動じゃなくて入力ミスだったときは？**  
　→ 履歴を削除または修正すればOKです。
    """)



# === ① 担当者の新規追加フォーム ===
st.subheader("➕ 担当者をUnitに追加（新規登録）")
st.markdown("""<br>""", unsafe_allow_html=True)

# まだ登録されていない担当者のみを抽出
already_assigned = set(current_df["担当者"].dropna())
unassigned_df = all_tantousha_df[~all_tantousha_df["担当者"].isin(already_assigned)]

if unassigned_df.empty:
    st.info("✨ CVリスト送信済みのすべての担当者はUnitに登録されています。")
else:
    selected_person = st.selectbox("👤 担当者を選択", unassigned_df["担当者"])
    input_unit = st.text_input("🏷️ 所属Unit名")
    input_status = st.text_input("💼 雇用形態（例：社員、インターン）")
    input_operator_id = st.text_input("🆔 マイページID（半角英小文字で苗字と名前の間にアンダーバー入力 例：takahashi_tsuyoshi）")
    input_start = st.text_input("📅 所属開始月 (YYYY-MM)", value=datetime.today().strftime("%Y-%m"))

    if st.button("＋ 担当者を追加"):
        if selected_person and input_unit and input_start:
            new_row = pd.DataFrame([{
                "担当者": selected_person,
                "所属": input_unit,
                "雇用形態": input_status,
                "operator_id": input_operator_id,
                "start_month": input_start,
                "end_month": None
            }])
            updated_df = pd.concat([current_df, new_row], ignore_index=True)
            save_to_bq(updated_df)
            st.success(f"✅ {selected_person} を {input_unit} に追加しました！")
            st.cache_data.clear()
            current_df = load_unit_mapping()
        else:
            st.warning("⚠️ 担当者・Unit・開始月は必須です")


# === ② Unit異動フォーム ===
st.subheader("🔁 Unit異動（上書きしない形式で更新）")
st.markdown("""<br>""", unsafe_allow_html=True)
with st.form("異動フォーム"):
    move_person = st.selectbox("👤 異動させる担当者", current_df[current_df["end_month"].isnull()]["担当者"].unique())
    new_unit = st.text_input("🏷️ 新しい所属Unit")
    move_month = st.text_input("📅 異動月 (YYYY-MM)", value=datetime.today().strftime("%Y-%m"))
    submitted = st.form_submit_button("異動を登録")
    if submitted:
        if move_person and new_unit and move_month:
            updated_df = current_df.copy()
            # 現所属のend_monthを埋める
            updated_df.loc[(updated_df["担当者"] == move_person) & (updated_df["end_month"].isnull()), "end_month"] = move_month
            # 新行を追加
            latest_row = updated_df[updated_df["担当者"] == move_person].sort_values("start_month").iloc[-1]
            new_row = latest_row.copy()
            new_row["所属"] = new_unit
            new_row["start_month"] = move_month
            new_row["end_month"] = None
            updated_df = pd.concat([updated_df, pd.DataFrame([new_row])], ignore_index=True)
            save_to_bq(updated_df)
            st.success(f"✅ {move_person} を {new_unit} に異動登録しました！")
            st.cache_data.clear()
            current_df = load_unit_mapping()
        else:
            st.warning("⚠️ 異動先Unitと異動月は必須です")

# === ④ Unitごとの現在の担当者一覧 ===
st.subheader("🏷️ Unitごとの現在の担当者一覧")
st.markdown("""<br>""", unsafe_allow_html=True)
current_only = current_df[current_df["end_month"].isnull()].copy()
for unit in sorted(current_only["所属"].dropna().unique()):
    st.markdown(f"#### 🔹 {unit}")
    st.dataframe(current_only[current_only["所属"] == unit][["担当者", "雇用形態"]], use_container_width=True)

# === ③ 編集・保存テーブル ===
st.subheader("📝 一覧編集（直接修正可）")
st.markdown("""<br>""", unsafe_allow_html=True)
editable_df = st.data_editor(
    current_df.sort_values(["担当者", "start_month"]),
    use_container_width=True,
    num_rows="dynamic",
    column_config={
        "operator_id": "マイページID",
        "start_month": "開始月",
        "end_month": "終了月"
    }
)
if st.button("💾 修正内容を保存"):
    save_to_bq(editable_df)
    st.success("✅ 編集内容を保存しました")
    st.cache_data.clear()
    current_df = load_unit_mapping()

# === ⑤ 異動履歴 ===
st.subheader("📜 過去の異動履歴")
st.markdown("""<br>""", unsafe_allow_html=True)
history_only = current_df[current_df["end_month"].notnull()].copy()
history_only = history_only.rename(columns={
    "start_month": "開始月",
    "end_month": "終了月",
    "operator_id": "マイページID"
})
st.dataframe(history_only.sort_values(["担当者", "開始月"]), use_container_width=True)

# === ⑥ メンバー外一覧（任意表示） ===
st.subheader("🚪 メンバー外（所属なし）担当者一覧")
st.markdown("""<br>""", unsafe_allow_html=True)
retired = current_df.groupby("担当者").agg(max_end=("end_month", "max"))
latest_start = current_df.groupby("担当者").agg(max_start=("start_month", "max"))
retired = retired.join(latest_start)
retired = retired[retired["max_end"] < datetime.today().strftime("%Y-%m")]
retired = retired.reset_index()
retired = retired.rename(columns={
    "operator_name": "担当者",
    "max_end": "終了月",
    "max_start": "最終開始月"
})
st.dataframe(retired, use_container_width=True)
