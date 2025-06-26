import streamlit as st
import pandas as pd
from google.cloud import bigquery
import html

# ──────────────────────────────────────────────
# ログイン認証
# ──────────────────────────────────────────────
from auth import require_login
require_login()

st.markdown("###### じゅんびちゅうです。")
