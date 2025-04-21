# --- 保存処理 ---
if st.button("💾 保存する"):
    with st.spinner("保存中..."):
        try:
            save_columns = [
                "カテゴリ", "広告目的",
                "CPA_best", "CPA_good", "CPA_min",
                "CVR_best", "CVR_good", "CVR_min",
                "CTR_best", "CTR_good", "CTR_min",
                "CPC_best", "CPC_good", "CPC_min",
                "CPM_best", "CPM_good", "CPM_min"
            ]
            save_df = edited_df[save_columns]
            save_df.to_gbq(
                destination_table=target_table,
                project_id=project_id,
                if_exists="replace",
                credentials=credentials
            )
            st.success("✅ データの保存に成功しました！")
            st.cache_data.clear()

        except Exception as e:
            st.error("❌ 保存に失敗しました。内容をご確認ください。")
            st.exception(e)
