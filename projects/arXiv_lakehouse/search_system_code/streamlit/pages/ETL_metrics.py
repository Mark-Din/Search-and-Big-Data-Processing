import streamlit as st
import pandas as pd
from mysql.connector import connect
import altair as alt
import sys
sys.path.append('/app/common')
from init_log import initlog

logger = initlog(__name__)

# ---------------------- MySQL connection ---------------------- #
def mysql_connection():
    conn = connect(
        host='mysql_db_container',
        port='3306',
        user='root',
        password='!QAZ2wsx',
        database='arXiv',
        connection_timeout=600
    )
    return conn


@st.cache_data(ttl=60)
def load_metadata():

    try:
        conn = mysql_connection()
        if not conn:
            st.warning('database connection not found')

        df = pd.read_sql("SELECT * FROM pipeline_metadata ORDER BY created_at DESC", conn)
        conn.close()
        return df
    except Exception as e:
        st.warning("Database not setup")
        raise

# ---------------------- Streamlit app ---------------------- #
def main():
    st.set_page_config(page_title="ETL Metrics Dashboard", layout="wide")
    st.sidebar.title("ETL Monitoring Dashboard")
    st.sidebar.markdown("Monitor your pipeline runs in real time.")

    try:

        df = load_metadata()
        
        if df.empty:
            st.warning("No pipeline metadata available.")
            return

        # Filter by date range
        with st.sidebar:
            max_date = df['created_at'].max().date()
            min_date = df['created_at'].min().date()
            st.markdown("### Filters")
            start_date = st.date_input("Start Date", min_date)
            end_date = st.date_input("End Date", max_date)
            status_filter = st.multiselect("Status", options=df['status'].unique(), default=df['status'].unique())

        logger.info(f'date unique====={type(df["created_at"].unique()[0].date())}')
        logger.info(f'end_date===={type(end_date)}')


        if not status_filter:
            st.warning('Please Select at least one attribute in Status column')
            return 
        elif start_date > max_date or end_date < min_date:
            st.warning('Please Select the right date range')
            return
        # elif any([date for date in df['created_at'].unique() if date.date() > start_date and date.date() < end_date]):
        #     st.warning('No data found in that date range')
        #     return
        

        # Filter data
        mask = (
            (df['created_at'].dt.date >= start_date)
            & (df['created_at'].dt.date <= end_date)
            & (df['status'].isin(status_filter))
        )

        df = df[mask]
        

        # ---------------- Latest run summary ---------------- #
        latest_run_id = df['run_id'].iloc[0]
        latest_run = df[df['run_id'] == latest_run_id]

        st.subheader(f"📊 Latest Run Summary — {latest_run_id}")
        col1, col2, col3, col4 = st.columns(4)
        total_records = latest_run['processed_records'].sum()
        total_duration = latest_run['duration_sec'].sum()
        total_stages = len(latest_run)
        success_count = (latest_run['status'] == 'S').sum()

        col1.metric("Stages", total_stages)
        col2.metric("Total Records", f"{total_records:,}")
        col3.metric("Total Duration (s)", round(total_duration, 2))
        col4.metric("Successful Stages", f"{success_count}/{total_stages}")

        # ---------------- Pipeline Health & Performance ---------------- #
        st.markdown("### 💡 Pipeline Health & Performance")

        success_rate = (df['status'] == 'S').mean() * 100
        avg_duration = df['duration_sec'].mean()
        avg_records = df['processed_records'].mean()

        col1, col2, col3 = st.columns(3)
        col1.metric("Success Rate", f"{success_rate:.1f}%")
        col2.metric("Avg Duration (s)", f"{avg_duration:.2f}")
        col3.metric("Avg Records per Stage", f"{avg_records:,.0f}")

        # ---------------- Stage details ---------------- #
        st.markdown("### 🔍 Stage Breakdown")
        st.dataframe(
            latest_run[['stage_name', 'component', 'processed_records', 'duration_sec', 'status', 'note', 'created_at']]
            .reset_index(drop=True)
        )

        # ---------------- Pipeline Run History ---------------- #
        st.markdown("### 🕒 Pipeline Run History")

        history = (
            df.groupby("run_id")
            .agg({
                "created_at": 'first',
                "processed_records": "sum",
                "duration_sec": "first",
                "status": lambda x: ','.join(sorted(set(x)))
            })
        )
        history.columns = ["start_time", "total_records", "total_duration", "statuses"]
        st.dataframe(history.reset_index())

        # ---------------- Trend over time ---------------- #
        st.markdown("### 📈 Trend Over Time")

        trend_chart = (
            alt.Chart(df)
            .mark_line(point=True)
            .encode(
                x=alt.X("created_at:T", title="Created At"),
                y=alt.Y("processed_records:Q", title="Processed Records"),
                color="stage_name:N",
                tooltip=["run_id", "stage_name", "processed_records", "duration_sec", "status"]
            )
            .properties(height=300)
        )
        st.altair_chart(trend_chart, use_container_width=True)

        # ---------------- Status summary ---------------- #
        st.markdown("### ⚠️ Status Overview")
        status_summary = df.groupby("status").size().reset_index(name="count")
        st.bar_chart(status_summary.set_index("status"))

    except Exception as e:
        st.error(f"Error loading data: {e}")
        logger.error(f"ETL_metrics error: {e}", exc_info=True)
    finally:
        try:
            conn.close()
        except:
            pass


if __name__ == "__main__":
    main()
