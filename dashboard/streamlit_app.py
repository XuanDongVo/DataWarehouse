import streamlit as st
import pandas as pd
import altair as alt
import db  # file db.py ở cùng thư mục

st.set_page_config(page_title="Real Estate DataMart", layout="wide")

# ========== CUSTOM CSS CHO GIAO DIỆN ==========
st.markdown(
    """
    <style>
    /* Nền tổng thể màu pastel */
    [data-testid="stAppViewContainer"] {
        background-color: #f5f7ff;
    }
    /* Ẩn nền header mặc định */
    [data-testid="stHeader"] {
        background-color: rgba(0,0,0,0);
    }
    .block-container {
        padding-top: 1rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }
    /* Màu tiêu đề */
    h1, h2, h3 {
        color: #1f2937;
    }
    /* Card trắng có đổ bóng nhẹ */
    .card {
        background-color: #ffffff;
        padding: 1.2rem 1rem;
        border-radius: 0.8rem;
        box-shadow: 0 2px 6px rgba(15, 23, 42, 0.08);
        border: 1px solid #e5e7eb;
        margin-top: 0.8rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🏡 Real Estate Dashboard (Data Mart)")


# =============================
# HÀM LOAD DỮ LIỆU
# =============================
@st.cache_data
def load_price_trends():
    return db.query_df("SELECT * FROM dim_price_trends_daily")


@st.cache_data
def load_property_features():
    return db.query_df("SELECT * FROM dim_property_features_daily")


@st.cache_data
def load_sales_daily():
    return db.query_df("SELECT * FROM dim_sales_daily")


# =============================
# TABS
# =============================
tab1, tab2, tab3 = st.tabs(
    ["📈 Price Trends Daily", "🏘 Property Features Daily", "💰 Sales Daily"]
)


# ----------------------------------------------------
# HÀM TẠO SLIDER AN TOÀN
# ----------------------------------------------------
def date_filter_block(df, label, slider_key=None):
    if "date_key" not in df.columns:
        return df

    min_date = int(df["date_key"].min())
    max_date = int(df["date_key"].max())

    # Nếu chỉ có 1 ngày thì trả df luôn, khỏi tạo slider
    if min_date == max_date:
        st.info(f"{label}: chỉ có 1 giá trị date_key = {min_date}")
        return df

    from_date, to_date = st.slider(
        label,
        min_value=min_date,
        max_value=max_date,
        value=(min_date, max_date),
        key=slider_key,
    )

    df = df[(df["date_key"] >= from_date) & (df["date_key"] <= to_date)]
    return df


# =============================
# TAB 1 – PRICE TRENDS DAILY (BAR CHART)
# =============================
with tab1:
    st.subheader("📈 Price Trends Daily")

    st.markdown('<div class="card">', unsafe_allow_html=True)

    df = load_price_trends()
    df = date_filter_block(df, "Khoảng thời gian", "price_slider")

    # Bar chart theo price_range & price_per_sqm_avg
    required_cols = {"price_range", "price_per_sqm_avg"}
    if required_cols.issubset(df.columns) and not df.empty:
        chart_df = df[["price_range", "price_per_sqm_avg", "property_count"]].copy()

        chart = (
            alt.Chart(chart_df)
            .mark_bar()
            .encode(
                x=alt.X("price_range:N", title="Khoảng giá (tỷ VNĐ)"),
                y=alt.Y("price_per_sqm_avg:Q", title="Giá trung bình / m² (triệu)"),
                color=alt.Color(
                    "price_range:N",
                    legend=None,
                    scale=alt.Scale(
                        range=["#4a90e2", "#50e3c2", "#f5a623", "#e94e77", "#9b59b6"]
                    ),
                ),
                tooltip=[
                    alt.Tooltip("price_range:N", title="Khoảng giá"),
                    alt.Tooltip("price_per_sqm_avg:Q", title="Giá TB / m²"),
                    alt.Tooltip("property_count:Q", title="Số tin đăng"),
                ],
            )
            .properties(height=400, title="Giá TB / m² theo khoảng giá")
        )

        st.altair_chart(chart, use_container_width=True)
    else:
        st.info(
            "Không đủ dữ liệu để vẽ biểu đồ bar chart "
            "(`price_range` & `price_per_sqm_avg`)."
        )

    st.markdown("</div>", unsafe_allow_html=True)


# =============================
# TAB 2 – PROPERTY FEATURES DAILY (BAR CHART)
# =============================
with tab2:
    st.subheader("🏘 Property Features Daily")
    st.markdown('<div class="card">', unsafe_allow_html=True)

    df = load_property_features()
    df = date_filter_block(df, "Khoảng thời gian", "features_slider")

    # Bar chart theo bedroom_range, hiển thị 2 chỉ tiêu:
    #   - avg_price_per_sqm
    #   - total_area
    required_cols = {"bedroom_range", "avg_price_per_sqm", "total_area"}
    if required_cols.issubset(df.columns) and not df.empty:
        base_df = df[["bedroom_range", "avg_price_per_sqm", "total_area"]].copy()

        # Chuyển từ wide sang long để vẽ nhiều metric trên cùng 1 biểu đồ
        melted = base_df.melt(
            id_vars="bedroom_range",
            var_name="metric",
            value_name="value",
        )

        chart = (
            alt.Chart(melted)
            .mark_bar()
            .encode(
                x=alt.X("bedroom_range:N", title="Số phòng ngủ"),
                y=alt.Y("value:Q", title="Giá trị"),
                color=alt.Color(
                    "metric:N",
                    title="Chỉ tiêu",
                    scale=alt.Scale(
                        range=["#4a90e2", "#f5a623"]  # 2 màu cho 2 metric
                    ),
                ),
                tooltip=[
                    alt.Tooltip("bedroom_range:N", title="Số phòng ngủ"),
                    alt.Tooltip("metric:N", title="Chỉ tiêu"),
                    alt.Tooltip("value:Q", title="Giá trị"),
                ],
            )
            .properties(height=400, title="Đặc điểm nhà theo số phòng ngủ")
        )

        st.altair_chart(chart, use_container_width=True)
    else:
        st.info(
            "Không đủ dữ liệu để vẽ bar chart "
            "(`bedroom_range`, `avg_price_per_sqm`, `total_area`)."
        )

    st.markdown("</div>", unsafe_allow_html=True)


# =============================
# TAB 3 – SALES DAILY (BAR CHART)
# =============================
with tab3:
    st.subheader("💰 Sales Daily")
    st.markdown('<div class="card">', unsafe_allow_html=True)

    df = load_sales_daily()
    df = date_filter_block(df, "Khoảng thời gian", "sales_slider")

    # Bar chart theo date_key & total_revenue
    if {"date_key", "total_revenue"}.issubset(df.columns) and not df.empty:
        chart_df = df[["date_key", "total_revenue"]].copy()

        chart = (
            alt.Chart(chart_df)
            .mark_bar()
            .encode(
                x=alt.X("date_key:O", title="Date key"),
                y=alt.Y("total_revenue:Q", title="Total revenue"),
                color=alt.value("#e94e77"),
                tooltip=[
                    alt.Tooltip("date_key:O", title="Ngày"),
                    alt.Tooltip("total_revenue:Q", title="Doanh thu"),
                ],
            )
            .properties(height=400, title="Doanh thu theo ngày")
        )

        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Không đủ dữ liệu để vẽ biểu đồ 'total_revenue'.")

    st.markdown("</div>", unsafe_allow_html=True)
