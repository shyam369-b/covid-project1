import streamlit as st
import pandas as pd
from pyathena import connect

# --------------------------
# Athena connection
# --------------------------
athena_region = "us-east-1"
s3_output = "s3://my-covid19-analytics-bucket/athena-results/"

conn = connect(region_name=athena_region, s3_staging_dir=s3_output)

# --------------------------
# Helper: Load state names
# --------------------------
@st.cache_data
def get_states():
    query = "SELECT abbr, name FROM covid_bronze_db.states_abv ORDER BY name"
    df = pd.read_sql(query, conn)
    return df

states_df = get_states()
state_options = states_df["name"].tolist()

# --------------------------
# Streamlit Layout
# --------------------------
st.set_page_config(page_title="COVID-19 Analytics AWS", layout="wide")
st.title("📊 COVID-19 Analytics Dashboard AWS (Athena)")

# Sidebar navigation
page = st.sidebar.radio(
    "Choose a view:",
    [
        "Overview",
        "Cases Trends",
        "Testing Trends",
        "Top States Analysis",
        "Combined Dashboard",
        "Dimensional Tables"
    ]
)

# --------------------------
# Page 1: Overview
# --------------------------
if page == "Overview":
    st.header("Pipeline Overview")
    st.markdown("""
    ✅ **Bronze → Silver → Gold pipeline** successfully built on AWS.  
    - **Bronze:** Raw CSVs from NYTimes & COVID Tracking Project  
    - **Silver:** Standardized Parquet (cases & testing)  
    - **Gold:** Fact & Dimension tables  
    - **Interface:** Streamlit (instead of QuickSight)  
    """)
    st.success("Data is live and ready for analytics!")

# --------------------------
# Page 2: Cases Trends (multi-state + 7-day avg)
# --------------------------
elif page == "Cases Trends":
    st.header("🦠 COVID Cases Trends")

    selected_states = st.multiselect("Select States", state_options, default=["New York", "California"])

    if selected_states:
        for state_name in selected_states:
            state_code = states_df.loc[states_df["name"] == state_name, "abbr"].values[0]
            query = f"""
                SELECT date_id, new_cases, new_deaths
                FROM covid_gold_db.fact_cases_state_daily
                WHERE state_code = '{state_code}'
                ORDER BY date_id
                LIMIT 500
            """
            df = pd.read_sql(query, conn)
            if not df.empty:
                df["date"] = pd.to_datetime(df["date_id"], format="%Y%m%d")
                df["date"] = df["date"].dt.strftime("%Y-%m-%d")

                # Add 7-day rolling averages
                df["new_cases_7day"] = df["new_cases"].rolling(7).mean()
                df["new_deaths_7day"] = df["new_deaths"].rolling(7).mean()

                st.subheader(f"📈 {state_name}")
                st.line_chart(df.set_index("date")[["new_cases", "new_cases_7day", "new_deaths_7day"]])
            else:
                st.warning(f"No data found for {state_name}")

# --------------------------
# Page 3: Testing Trends (multi-state + 7-day avg)
# --------------------------
elif page == "Testing Trends":
    st.header("🧪 COVID Testing Trends")

    selected_states = st.multiselect("Select States", state_options, default=["Texas", "Florida"])

    if selected_states:
        for state_name in selected_states:
            state_code = states_df.loc[states_df["name"] == state_name, "abbr"].values[0]
            query = f"""
                SELECT date_id, new_tests, positivity_rate
                FROM covid_gold_db.fact_testing_state_daily
                WHERE state_code = '{state_code}'
                ORDER BY date_id
                LIMIT 500
            """
            df = pd.read_sql(query, conn)
            if not df.empty:
                df["date"] = pd.to_datetime(df["date_id"], format="%Y%m%d")
                df["date"] = df["date"].dt.strftime("%Y-%m-%d")

                # Add 7-day rolling averages
                df["new_tests_7day"] = df["new_tests"].rolling(7).mean()
                df["positivity_rate_7day"] = df["positivity_rate"].rolling(7).mean()

                st.subheader(f"📈 {state_name}")
                st.line_chart(df.set_index("date")[["new_tests_7day", "positivity_rate_7day"]])
            else:
                st.warning(f"No data found for {state_name}")

# --------------------------
# Page 4: Top States Analysis
# --------------------------
elif page == "Top States Analysis":
    st.header("🏆 Top States Analysis")

    # Date picker instead of text input
    selected_date = st.date_input("Select Date", value=pd.to_datetime("2020-05-01"))
    date = selected_date.strftime("%Y%m%d")  # Convert to YYYYMMDD for Athena

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top 5 States by New Cases")
        query_cases = f"""
            SELECT t.state_code, s.name as state_name, new_cases
            FROM covid_gold_db.fact_cases_state_daily t
            JOIN covid_bronze_db.states_abv s
              ON t.state_code = s.abbr
            WHERE date_id = '{date}'
            ORDER BY new_cases DESC
            LIMIT 5
        """
        df_cases = pd.read_sql(query_cases, conn)
        if not df_cases.empty:
            st.bar_chart(df_cases.set_index("state_name")["new_cases"])
        else:
            st.warning("No case data available for this date.")

    with col2:
        st.subheader("Top 5 States by Positivity Rate")
        query_pos = f"""
            SELECT t.state_code, s.name as state_name, positivity_rate
            FROM covid_gold_db.fact_testing_state_daily t
            JOIN covid_bronze_db.states_abv s
              ON t.state_code = s.abbr
            WHERE date_id = '{date}'
            ORDER BY positivity_rate DESC
            LIMIT 5
        """
        df_pos = pd.read_sql(query_pos, conn)
        if not df_pos.empty:
            st.bar_chart(df_pos.set_index("state_name")["positivity_rate"])
        else:
            st.warning("No testing data available for this date.")


# --------------------------
# Page 5: Combined Dashboard (multi-state + 7-day avg)
# --------------------------
elif page == "Combined Dashboard":
    st.header("📊 Combined View: Cases + Testing")

    selected_states = st.multiselect("Select States", state_options, default=["Texas", "New York"])

    if selected_states:
        for state_name in selected_states:
            state_code = states_df.loc[states_df["name"] == state_name, "abbr"].values[0]

            query_cases = f"""
                SELECT date_id, new_cases, new_deaths
                FROM covid_gold_db.fact_cases_state_daily
                WHERE state_code = '{state_code}'
                ORDER BY date_id
                LIMIT 500
            """
            df_cases = pd.read_sql(query_cases, conn)

            query_tests = f"""
                SELECT date_id, new_tests, positivity_rate
                FROM covid_gold_db.fact_testing_state_daily
                WHERE state_code = '{state_code}'
                ORDER BY date_id
                LIMIT 500
            """
            df_tests = pd.read_sql(query_tests, conn)

            if not df_cases.empty and not df_tests.empty:
                df_cases["date"] = pd.to_datetime(df_cases["date_id"], format="%Y%m%d")
                df_cases["date"] = df_cases["date"].dt.strftime("%Y-%m-%d")
                df_tests["date"] = pd.to_datetime(df_tests["date_id"], format="%Y%m%d")
                df_tests["date"] = df_tests["date"].dt.strftime("%Y-%m-%d")

                # Rolling averages
                df_cases["new_cases_7day"] = df_cases["new_cases"].rolling(7).mean()
                df_tests["new_tests_7day"] = df_tests["new_tests"].rolling(7).mean()
                df_tests["positivity_rate_7day"] = df_tests["positivity_rate"].rolling(7).mean()

                col1, col2 = st.columns(2)
                with col1:
                    st.subheader(f"🦠 Cases & Deaths in {state_name}")
                    st.line_chart(df_cases.set_index("date")[["new_cases_7day", "new_deaths"]])
                with col2:
                    st.subheader(f"🧪 Testing & Positivity in {state_name}")
                    st.line_chart(df_tests.set_index("date")[["new_tests_7day", "positivity_rate_7day"]])
            else:
                st.warning(f"No combined data found for {state_name}")

# --------------------------
# Page 6: Dimensional Tables
# --------------------------
elif page == "Dimensional Tables":
    st.header("📐 Dimension Tables (Reference Data)")

    st.subheader("dim_date (first 10 rows)")
    df_date = pd.read_sql("SELECT * FROM covid_gold_db.dim_date LIMIT 10", conn)
    st.dataframe(df_date)

    st.subheader("dim_state (all rows)")
    df_state = pd.read_sql("SELECT * FROM covid_gold_db.dim_state", conn)
    st.dataframe(df_state)

    st.subheader("states_abv lookup (all rows)")
    df_lookup = pd.read_sql("SELECT * FROM covid_bronze_db.states_abv", conn)
    st.dataframe(df_lookup)
