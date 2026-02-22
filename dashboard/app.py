# ============================================================
# Kommineni Automotive - Clean Light Dashboard
# White + Dark text + Gold | Top filters | No sidebar
# ============================================================

import streamlit as st
import duckdb
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, date, timedelta
import os

st.set_page_config(
    page_title="Kommineni Automotive",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

* {
    font-family: 'Sora', sans-serif !important;
    box-sizing: border-box;
}

/* White background */
.stApp {
    background: #FAFAFA !important;
}

/* Hide sidebar completely */
[data-testid="stSidebar"] { display: none !important; }
[data-testid="collapsedControl"] { display: none !important; }

/* Hide streamlit chrome */
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
header { visibility: hidden; }

/* Main content full width */
.block-container {
    padding: 0 !important;
    max-width: 100% !important;
}

/* Metric cards */
[data-testid="metric-container"] {
    background: white !important;
    border: 1px solid #EBEBEB !important;
    border-radius: 10px !important;
    padding: 20px 18px !important;
    position: relative;
    overflow: hidden;
    box-shadow: none !important;
}

[data-testid="metric-container"]::before {
    content: '';
    position: absolute;
    top: 0; left: 20px; right: 20px;
    height: 2px;
    background: #B8860B;
    border-radius: 0 0 2px 2px;
}

[data-testid="stMetricValue"] {
    font-size: 1.7rem !important;
    font-weight: 700 !important;
    color: #1A1A1A !important;
    letter-spacing: -1px !important;
    font-family: 'Sora', sans-serif !important;
}

[data-testid="stMetricLabel"] {
    font-size: 10px !important;
    font-weight: 600 !important;
    color: #999 !important;
    text-transform: uppercase !important;
    letter-spacing: 1.5px !important;
    font-family: 'Sora', sans-serif !important;
}

[data-testid="stMetricDelta"] {
    font-size: 11px !important;
    font-family: 'Sora', sans-serif !important;
}

[data-testid="stMetricDelta"] svg { display: none !important; }

/* Dataframes */
[data-testid="stDataFrame"] {
    border: 1px solid #EBEBEB !important;
    border-radius: 8px !important;
}

/* Selectbox */
.stSelectbox > div > div {
    background: white !important;
    border: 1px solid #EBEBEB !important;
    border-radius: 6px !important;
    color: #666 !important;
    font-size: 12px !important;
}

/* Radio buttons */
.stRadio > div {
    display: flex !important;
    flex-direction: row !important;
    gap: 6px !important;
    flex-wrap: wrap !important;
}

.stRadio label {
    background: white !important;
    border: 1px solid #EBEBEB !important;
    border-radius: 20px !important;
    padding: 5px 14px !important;
    font-size: 12px !important;
    color: #666 !important;
    cursor: pointer !important;
    transition: all 0.15s !important;
}

.stRadio label:has(input:checked) {
    background: #FFFBEF !important;
    border-color: #B8860B !important;
    color: #B8860B !important;
    font-weight: 600 !important;
}

/* Buttons */
.stButton > button {
    background: #1A1A1A !important;
    color: white !important;
    border: none !important;
    border-radius: 6px !important;
    font-size: 11px !important;
    font-weight: 600 !important;
    letter-spacing: 2px !important;
    text-transform: uppercase !important;
    padding: 10px 20px !important;
    font-family: 'Sora', sans-serif !important;
    transition: all 0.2s !important;
}

.stButton > button:hover {
    background: #B8860B !important;
    color: #1A1A1A !important;
}

/* Text inputs */
.stTextInput > div > input {
    background: #FAFAFA !important;
    border: 1px solid #EBEBEB !important;
    border-radius: 6px !important;
    color: #1A1A1A !important;
    font-size: 13px !important;
    padding: 11px 14px !important;
    font-family: 'Sora', sans-serif !important;
}

.stTextInput > div > input:focus {
    border-color: #B8860B !important;
    box-shadow: 0 0 0 3px rgba(184,134,11,0.08) !important;
}

.stTextInput label {
    font-size: 10px !important;
    font-weight: 600 !important;
    color: #999 !important;
    text-transform: uppercase !important;
    letter-spacing: 1.5px !important;
}

/* Alert/error */
.stAlert {
    border-radius: 6px !important;
    font-size: 13px !important;
}
</style>
""", unsafe_allow_html=True)

# ============================================================
# DATABASE
# ============================================================

DB_PATH = os.path.join(
    os.path.dirname(__file__),
    "../kommineni_automotive.duckdb"
)

@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)

conn = get_connection()

def query(sql):
    return conn.execute(sql).df()

# ============================================================
# CHART THEME
# ============================================================

CHART = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#999", family="Sora", size=11),
    margin=dict(t=10, b=10, l=10, r=10),
    xaxis=dict(
        gridcolor="#F5F5F5",
        color="#BBB",
        showline=False,
        tickfont=dict(size=10)
    ),
    yaxis=dict(
        gridcolor="#F5F5F5",
        color="#BBB",
        showline=False,
        tickfont=dict(size=10)
    ),
    legend=dict(
        bgcolor="rgba(0,0,0,0)",
        font=dict(color="#999", size=10)
    )
)

GOLD = "#B8860B"
GOLD_BG = "#FFFBEF"
GREEN = "#16A34A"
RED = "#DC2626"
CHARCOAL = "#1A1A1A"

# ============================================================
# USERS
# ============================================================

@st.cache_data
def load_salespeople():
    return conn.execute("""
        SELECT employee_id, full_name, location_id
        FROM main_silver.stg_employees
        WHERE is_salesperson = TRUE
        ORDER BY full_name
    """).df()

LOCATION_MAP = {
    "All Locations": None,
    "Dallas": "LOC001",
    "Chicago": "LOC002",
    "Atlanta": "LOC003",
    "Phoenix": "LOC004",
    "Seattle": "LOC005"
}

USERS = {
    "exec001": {"password": "exec001", "role": "Executive",
                "name": "Prameel Kommineni", "location_id": None,
                "employee_id": None, "city": None},
    "mgr001": {"password": "mgr001", "role": "Branch Manager",
               "name": "Marcus Johnson", "location_id": "LOC001",
               "employee_id": None, "city": "Dallas"},
    "mgr002": {"password": "mgr002", "role": "Branch Manager",
               "name": "Sarah Mitchell", "location_id": "LOC002",
               "employee_id": None, "city": "Chicago"},
    "mgr003": {"password": "mgr003", "role": "Branch Manager",
               "name": "David Reyes", "location_id": "LOC003",
               "employee_id": None, "city": "Atlanta"},
    "mgr004": {"password": "mgr004", "role": "Branch Manager",
               "name": "Linda Park", "location_id": "LOC004",
               "employee_id": None, "city": "Phoenix"},
    "mgr005": {"password": "mgr005", "role": "Branch Manager",
               "name": "James Okafor", "location_id": "LOC005",
               "employee_id": None, "city": "Seattle"},
}

sp_df = load_salespeople()
for _, row in sp_df.iterrows():
    USERS[row["employee_id"]] = {
        "password": row["employee_id"],
        "role": "Salesperson",
        "name": row["full_name"],
        "location_id": row["location_id"],
        "employee_id": row["employee_id"],
        "city": None
    }

# ============================================================
# SESSION STATE
# ============================================================

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "user" not in st.session_state:
    st.session_state.user = None

# ============================================================
# HELPERS
# ============================================================

def section_title(text):
    st.markdown(
        f'<p style="font-size:10px;font-weight:600;color:#999;'
        f'text-transform:uppercase;letter-spacing:1.5px;'
        f'margin:0 0 14px 0">{text}</p>',
        unsafe_allow_html=True
    )

def card_start():
    st.markdown(
        '<div style="background:white;border:1px solid #EBEBEB;'
        'border-radius:10px;padding:24px">',
        unsafe_allow_html=True
    )

def card_end():
    st.markdown('</div>', unsafe_allow_html=True)

def kpi_card(label, value, sub=None, sub_color="#999"):
    return f"""
    <div style="background:white;border:1px solid #EBEBEB;
                border-radius:10px;padding:20px 18px;
                position:relative;overflow:hidden;
                transition:box-shadow 0.2s">
        <div style="position:absolute;top:0;left:20px;right:20px;
                    height:2px;background:{GOLD};
                    border-radius:0 0 2px 2px"></div>
        <p style="font-size:10px;font-weight:600;color:#999;
                  text-transform:uppercase;letter-spacing:1.5px;
                  margin:0 0 10px 0">{label}</p>
        <p style="font-size:1.65rem;font-weight:700;color:#1A1A1A;
                  letter-spacing:-1px;margin:0;line-height:1">{value}</p>
        {f'<p style="font-size:11px;color:{sub_color};margin:6px 0 0 0;font-weight:500">{sub}</p>' if sub else ''}
    </div>
    """

def divider():
    st.markdown(
        '<div style="height:1px;background:#EBEBEB;margin:20px 0"></div>',
        unsafe_allow_html=True
    )

def build_where(f, a="s"):
    parts = [
        f"{a}.sale_date_only >= '{f['start_date']}'",
        f"{a}.sale_date_only <= '{f['end_date']}'"
    ]
    if f.get("location_id"):
        parts.append(f"{a}.location_id = '{f['location_id']}'")
    if f.get("sales_type") == "Financed":
        parts.append(f"{a}.financing_approved = TRUE")
    elif f.get("sales_type") == "Cash":
        parts.append(f"{a}.financing_approved = FALSE")
    if f.get("salesperson_id"):
        parts.append(f"{a}.employee_id = '{f['salesperson_id']}'")
    return "WHERE " + " AND ".join(parts)

# ============================================================
# LOGIN
# ============================================================

def show_login():
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        st.markdown("<div style='height:80px'></div>", unsafe_allow_html=True)

        st.markdown("""
        <div style="text-align:center;margin-bottom:40px">
            <p style="font-size:22px;font-weight:700;color:#1A1A1A;
                      letter-spacing:-0.5px;margin:0">
                Kommineni Automotive
            </p>
            <div style="width:32px;height:2px;background:#B8860B;
                        margin:12px auto"></div>
            <p style="font-size:10px;color:#999;letter-spacing:2.5px;
                      text-transform:uppercase;margin:0;font-weight:600">
                Performance Intelligence
            </p>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("""
        <div style="background:white;border:1px solid #EBEBEB;
                    border-radius:12px;padding:36px 32px;
                    box-shadow:0 4px 40px rgba(0,0,0,0.06)">
            <p style="font-size:16px;font-weight:600;
                      color:#1A1A1A;margin:0 0 4px 0">Sign In</p>
            <p style="font-size:12px;color:#999;margin:0 0 24px 0">
                Enter your credentials to continue
            </p>
        </div>
        """, unsafe_allow_html=True)

        user_id = st.text_input(
            "User ID", placeholder="exec001 · mgr001 · EMP001"
        )
        password = st.text_input(
            "Password", type="password", placeholder="Password = User ID"
        )

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

        if st.button("Sign In", use_container_width=True):
            if user_id in USERS and USERS[user_id]["password"] == password:
                st.session_state.logged_in = True
                st.session_state.user = USERS[user_id]
                st.session_state.user["user_id"] = user_id
                st.rerun()
            else:
                st.error("Invalid credentials.")

        st.markdown("""
        <div style="margin-top:24px;padding-top:20px;
                    border-top:1px solid #EBEBEB;text-align:center">
            <p style="font-size:10px;color:#CCC;letter-spacing:1.5px;
                      text-transform:uppercase;margin:0 0 8px 0">
                Demo Access
            </p>
            <p style="font-size:12px;color:#999;margin:0;line-height:2.2">
                Executive
                <code style="background:#FFFBEF;color:#B8860B;
                             padding:1px 6px;border-radius:3px;
                             font-size:10px">exec001</code><br>
                Managers
                <code style="background:#FFFBEF;color:#B8860B;
                             padding:1px 6px;border-radius:3px;
                             font-size:10px">mgr001 – mgr005</code><br>
                Salespeople
                <code style="background:#FFFBEF;color:#B8860B;
                             padding:1px 6px;border-radius:3px;
                             font-size:10px">EMP001 – EMP020</code>
            </p>
        </div>
        """, unsafe_allow_html=True)

# ============================================================
# TOP NAV
# ============================================================

def show_topnav(user):
    st.markdown(f"""
    <div style="background:white;border-bottom:1px solid #EBEBEB;
                padding:0 48px;display:flex;align-items:center;
                justify-content:space-between;height:56px;
                position:sticky;top:0;z-index:100;
                margin:-1rem -1rem 0 -1rem">
        <div style="display:flex;align-items:center;gap:10px">
            <div style="width:6px;height:6px;background:#B8860B;
                        border-radius:50%"></div>
            <span style="font-size:14px;font-weight:700;
                         color:#1A1A1A;letter-spacing:-0.3px">
                Kommineni Automotive
            </span>
        </div>
        <div style="display:flex;align-items:center;gap:20px">
            <div style="text-align:right">
                <p style="font-size:12px;font-weight:500;
                          color:#1A1A1A;margin:0">{user['name']}</p>
                <p style="font-size:10px;color:#999;letter-spacing:1.5px;
                          text-transform:uppercase;margin:0">{user['role']}</p>
            </div>
            <div style="display:flex;align-items:center;gap:5px;
                        background:#F0FDF4;border:1px solid #BBF7D0;
                        border-radius:20px;padding:4px 10px">
                <div style="width:5px;height:5px;background:#16A34A;
                            border-radius:50%"></div>
                <span style="font-size:10px;color:#16A34A;
                             font-weight:600">Live</span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

# ============================================================
# FILTERS BAR
# ============================================================

def show_filters(user):
    st.markdown("""
    <div style="background:white;border-bottom:1px solid #EBEBEB;
                padding:12px 48px;margin:0 -1rem;
                display:flex;align-items:center;gap:8px;
                overflow-x:auto">
    </div>
    """, unsafe_allow_html=True)

    today = date.today()

    with st.container():
        col1, col2, col3, col4, col5, col6, col7 = st.columns(
            [0.8, 1.8, 0.1, 1.2, 0.1, 1.2, 1.2]
        )

        with col1:
            st.markdown(
                '<p style="font-size:10px;font-weight:600;color:#999;'
                'text-transform:uppercase;letter-spacing:1.5px;'
                'margin:8px 0 4px 0">Period</p>',
                unsafe_allow_html=True
            )

        with col2:
            date_option = st.radio(
                "Period",
                ["7 Days", "30 Days", "This Month"],
                index=1,
                horizontal=True,
                label_visibility="collapsed"
            )

        with col3:
            st.markdown(
                '<div style="height:36px;width:1px;background:#EBEBEB;'
                'margin-top:4px"></div>',
                unsafe_allow_html=True
            )

        with col4:
            if user["role"] == "Executive":
                location_filter = st.selectbox(
                    "Location",
                    list(LOCATION_MAP.keys()),
                    label_visibility="collapsed"
                )
                location_id_filter = LOCATION_MAP[location_filter]
            else:
                location_id_filter = user.get("location_id")
                st.markdown(
                    f'<p style="font-size:12px;color:#666;'
                    f'margin:8px 0 0 0">'
                    f'{user.get("city", "")} Branch</p>',
                    unsafe_allow_html=True
                )

        with col5:
            st.markdown(
                '<div style="height:36px;width:1px;background:#EBEBEB;'
                'margin-top:4px"></div>',
                unsafe_allow_html=True
            )

        with col6:
            makes = query("""
                SELECT DISTINCT make FROM main_silver.stg_vehicles
                ORDER BY make
            """)["make"].tolist()
            make_filter = st.selectbox(
                "Make",
                ["All Makes"] + makes,
                label_visibility="collapsed"
            )
            selected_make = (
                None if make_filter == "All Makes" else make_filter
            )

        with col7:
            sales_type = st.radio(
                "Sale Type",
                ["All", "Financed", "Cash"],
                horizontal=True,
                label_visibility="collapsed"
            )

    if date_option == "7 Days":
        start_date = today - timedelta(days=7)
        end_date = today
    elif date_option == "30 Days":
        start_date = today - timedelta(days=30)
        end_date = today
    else:
        start_date = today.replace(day=1)
        end_date = today

    sp_filter = None
    if user["role"] in ["Executive", "Branch Manager"]:
        with st.container():
            col1, col2, col3 = st.columns([1, 2, 4])
            with col1:
                st.markdown(
                    '<p style="font-size:10px;font-weight:600;color:#999;'
                    'text-transform:uppercase;letter-spacing:1.5px;'
                    'margin:4px 0">Salesperson</p>',
                    unsafe_allow_html=True
                )
            with col2:
                if user["role"] == "Branch Manager":
                    sp_options = sp_df[
                        sp_df["location_id"] == user["location_id"]
                    ]["full_name"].tolist()
                else:
                    sp_options = sp_df["full_name"].tolist()

                sp_choice = st.selectbox(
                    "Salesperson",
                    ["All Salespeople"] + sp_options,
                    label_visibility="collapsed"
                )
                if sp_choice != "All Salespeople":
                    sp_filter = sp_df[
                        sp_df["full_name"] == sp_choice
                    ]["employee_id"].values[0]

    return {
        "start_date": start_date,
        "end_date": end_date,
        "location_id": location_id_filter,
        "make": selected_make,
        "sales_type": sales_type,
        "salesperson_id": sp_filter
    }

# ============================================================
# EXECUTIVE DASHBOARD
# ============================================================

def show_executive(f):
    loc_label = "All Locations"
    if f.get("location_id"):
        loc_label = [
            k for k, v in LOCATION_MAP.items()
            if v == f["location_id"]
        ][0]

    st.markdown(f"""
    <div style="padding:32px 48px 20px 48px;margin:0 -1rem">
        <p style="font-size:1.5rem;font-weight:700;color:#1A1A1A;
                  letter-spacing:-0.5px;margin:0">
            Executive <span style="color:#B8860B">Overview</span>
        </p>
        <p style="font-size:12px;color:#999;margin:4px 0 0 0">
            {loc_label} &nbsp;·&nbsp;
            {f['start_date'].strftime('%b %d')} –
            {f['end_date'].strftime('%b %d, %Y')}
        </p>
    </div>
    """, unsafe_allow_html=True)

    where = build_where(f)

    rev = query(f"""
        SELECT COALESCE(SUM(s.sale_price), 0) as v
        FROM main_silver.stg_sales_transactions s {where}
    """)["v"].values[0]

    units = int(query(f"""
        SELECT COUNT(*) as v
        FROM main_silver.stg_sales_transactions s {where}
    """)["v"].values[0])

    avg_deal = (rev / units) if units > 0 else 0

    financed = int(query(f"""
        SELECT COUNT(*) as v
        FROM main_silver.stg_sales_transactions s
        WHERE s.sale_date_only >= '{f['start_date']}'
        AND s.sale_date_only <= '{f['end_date']}'
        {"AND s.location_id = '" + f['location_id'] + "'" if f.get('location_id') else ""}
        AND s.financing_approved = TRUE
    """)["v"].values[0])

    fin_rate = (financed / units * 100) if units > 0 else 0

    on_track = int(query("""
        SELECT COUNT(*) as v FROM main_gold.revenue_vs_target
        WHERE status = 'On Track'
    """)["v"].values[0])

    target_total = query("""
        SELECT COALESCE(SUM(monthly_target), 0) as v
        FROM main_gold.revenue_vs_target
    """)["v"].values[0]

    pct = (rev / target_total * 100) if target_total > 0 else 0

    # KPI Cards
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(kpi_card(
            "Total Revenue", f"${rev:,.0f}",
            f"{pct:.1f}% of target", GOLD
        ), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi_card(
            "Units Sold", str(units),
            f"avg ${avg_deal:,.0f} / deal"
        ), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi_card(
            "Avg Deal Size", f"${avg_deal:,.0f}"
        ), unsafe_allow_html=True)
    with c4:
        st.markdown(kpi_card(
            "Financing Rate", f"{fin_rate:.1f}%",
            f"{financed} financed deals"
        ), unsafe_allow_html=True)
    with c5:
        st.markdown(kpi_card(
            "On Track", f"{on_track} / 5",
            "branches hitting target",
            GREEN if on_track >= 3 else RED
        ), unsafe_allow_html=True)

    divider()

    # Revenue chart + progress bars
    col1, col2 = st.columns([3, 2])

    with col1:
        section_title("Revenue by Location")
        rev_loc = query(f"""
            SELECT l.city,
                   COALESCE(SUM(s.sale_price), 0) as revenue,
                   l.monthly_target
            FROM main_silver.stg_locations l
            LEFT JOIN main_silver.stg_sales_transactions s
                ON l.location_id = s.location_id
                AND s.sale_date_only >= '{f['start_date']}'
                AND s.sale_date_only <= '{f['end_date']}'
                {"AND l.location_id = '" + f['location_id'] + "'" if f.get('location_id') else ""}
            GROUP BY l.city, l.monthly_target
            ORDER BY revenue DESC
        """)
        fig = go.Figure()
        fig.add_bar(
            name="Revenue",
            x=rev_loc["city"],
            y=rev_loc["revenue"],
            marker=dict(color=GOLD, opacity=0.9),
            text=[f"${v/1e6:.2f}M" for v in rev_loc["revenue"]],
            textposition="outside",
            textfont=dict(size=10, color=GOLD)
        )
        fig.add_bar(
            name="Target",
            x=rev_loc["city"],
            y=rev_loc["monthly_target"],
            marker=dict(
                color="rgba(0,0,0,0)",
                line=dict(color="#DDD", width=1.5)
            )
        )
        fig.update_layout(
            barmode="overlay",
            height=260,
            showlegend=True,
            **CHART
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        section_title("Target Achievement")
        tgt = query("""
            SELECT city, pct_of_target, status
            FROM main_gold.revenue_vs_target
            ORDER BY pct_of_target DESC
        """)
        for _, row in tgt.iterrows():
            on = row["status"] == "On Track"
            color = GREEN if on else RED
            pct_display = min(row["pct_of_target"], 100)
            st.markdown(f"""
            <div style="margin-bottom:16px">
                <div style="display:flex;justify-content:space-between;
                            margin-bottom:6px">
                    <span style="font-size:12px;color:#1A1A1A;
                                 font-weight:500">{row['city']}</span>
                    <span style="font-size:11px;color:{color};
                                 font-weight:600;
                                 font-family:'JetBrains Mono',monospace">
                        {row['pct_of_target']:.1f}%
                    </span>
                </div>
                <div style="height:5px;background:#F0F0F0;
                            border-radius:3px;overflow:hidden">
                    <div style="height:100%;width:{pct_display}%;
                                background:{color};border-radius:3px">
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    divider()

    # Trend
    section_title("Daily Revenue Trend")
    trend = query(f"""
        SELECT s.sale_date_only as dt, SUM(s.sale_price) as revenue
        FROM main_silver.stg_sales_transactions s {where}
        GROUP BY dt ORDER BY dt
    """)
    fig2 = go.Figure()
    fig2.add_scatter(
        x=trend["dt"],
        y=trend["revenue"],
        fill="tozeroy",
        fillcolor="rgba(184,134,11,0.06)",
        line=dict(color=GOLD, width=2),
        mode="lines",
        name="Revenue"
    )
    fig2.update_layout(height=180, **CHART)
    st.plotly_chart(fig2, use_container_width=True)

    divider()

    # Leaderboard + Status
    col1, col2 = st.columns([3, 2])

    with col1:
        section_title("Salesperson Leaderboard")
        sp_parts = [
            f"s.sale_date_only >= '{f['start_date']}'",
            f"s.sale_date_only <= '{f['end_date']}'"
        ]
        if f.get("location_id"):
            sp_parts.append(f"s.location_id = '{f['location_id']}'")
        if f.get("salesperson_id"):
            sp_parts.append(f"e.employee_id = '{f['salesperson_id']}'")
        sp_where = "WHERE " + " AND ".join(sp_parts)

        board = query(f"""
            SELECT
                ROW_NUMBER() OVER (
                    ORDER BY SUM(s.sale_price) DESC
                ) as Rank,
                e.full_name as Name,
                l.city as Branch,
                COUNT(s.transaction_id) as Deals,
                ROUND(SUM(s.sale_price), 0) as Revenue,
                ROUND(SUM(s.sale_price) * e.commission_rate, 0) as Commission
            FROM main_silver.stg_employees e
            LEFT JOIN main_silver.stg_sales_transactions s
                ON e.employee_id = s.employee_id
            LEFT JOIN main_silver.stg_locations l
                ON e.location_id = l.location_id
            {sp_where} AND e.is_salesperson = TRUE
            GROUP BY e.employee_id, e.full_name,
                     l.city, e.commission_rate
            HAVING COUNT(s.transaction_id) > 0
            ORDER BY Revenue DESC
            LIMIT 10
        """)
        st.dataframe(board, use_container_width=True, hide_index=True)

    with col2:
        section_title("Branch Status")
        status_df = query("""
            SELECT city as Branch,
                   revenue_to_date as Revenue,
                   status as Status
            FROM main_gold.revenue_vs_target
            ORDER BY revenue_to_date DESC
        """)
        st.dataframe(
            status_df, use_container_width=True, hide_index=True
        )

    # Sign out button
    divider()
    col1, col2, col3 = st.columns([4, 1, 4])
    with col2:
        if st.button("Sign Out", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.user = None
            st.rerun()

# ============================================================
# BRANCH MANAGER
# ============================================================

def show_branch_manager(user, f):
    location_id = user["location_id"]
    city = user["city"]

    st.markdown(f"""
    <div style="padding:32px 48px 20px 48px;margin:0 -1rem">
        <p style="font-size:1.5rem;font-weight:700;color:#1A1A1A;
                  letter-spacing:-0.5px;margin:0">
            {city} <span style="color:#B8860B">Branch</span>
        </p>
        <p style="font-size:12px;color:#999;margin:4px 0 0 0">
            {user['name']} &nbsp;·&nbsp;
            {f['start_date'].strftime('%b %d')} –
            {f['end_date'].strftime('%b %d, %Y')}
        </p>
    </div>
    """, unsafe_allow_html=True)

    where = build_where(f)

    rev = query(f"""
        SELECT COALESCE(SUM(s.sale_price), 0) as v
        FROM main_silver.stg_sales_transactions s {where}
    """)["v"].values[0]

    units = int(query(f"""
        SELECT COUNT(*) as v
        FROM main_silver.stg_sales_transactions s {where}
    """)["v"].values[0])

    tgt_row = query(f"""
        SELECT monthly_target, pct_of_target, status
        FROM main_gold.revenue_vs_target
        WHERE location_id = '{location_id}'
    """)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(kpi_card(
            "Revenue", f"${rev:,.0f}", "period total"
        ), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi_card(
            "Units Sold", str(units)
        ), unsafe_allow_html=True)

    if len(tgt_row) > 0:
        row = tgt_row.iloc[0]
        with c3:
            st.markdown(kpi_card(
                "Monthly Target",
                f"${row['monthly_target']:,.0f}",
                f"{row['pct_of_target']:.1f}% achieved",
                GREEN if row["pct_of_target"] >= 80 else RED
            ), unsafe_allow_html=True)
        with c4:
            on = row["status"] == "On Track"
            st.markdown(kpi_card(
                "Status",
                "On Track" if on else "Behind",
                sub_color=GREEN if on else RED
            ), unsafe_allow_html=True)

    divider()

    col1, col2 = st.columns([3, 2])

    with col1:
        section_title("Daily Revenue")
        trend = query(f"""
            SELECT s.sale_date_only as dt, SUM(s.sale_price) as revenue
            FROM main_silver.stg_sales_transactions s {where}
            GROUP BY dt ORDER BY dt
        """)
        fig = go.Figure()
        fig.add_bar(
            x=trend["dt"],
            y=trend["revenue"],
            marker=dict(color=GOLD, opacity=0.85)
        )
        fig.update_layout(height=240, **CHART)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        section_title("Team Performance")
        sp_parts = [
            f"s.sale_date_only >= '{f['start_date']}'",
            f"s.sale_date_only <= '{f['end_date']}'",
            f"e.location_id = '{location_id}'"
        ]
        if f.get("salesperson_id"):
            sp_parts.append(
                f"e.employee_id = '{f['salesperson_id']}'"
            )
        sp_where = "WHERE " + " AND ".join(sp_parts)

        team = query(f"""
            SELECT e.full_name as Name,
                   COUNT(s.transaction_id) as Deals,
                   ROUND(COALESCE(SUM(s.sale_price), 0), 0) as Revenue
            FROM main_silver.stg_employees e
            LEFT JOIN main_silver.stg_sales_transactions s
                ON e.employee_id = s.employee_id
            {sp_where} AND e.is_salesperson = TRUE
            GROUP BY e.employee_id, e.full_name
            ORDER BY Revenue DESC
        """)
        st.dataframe(team, use_container_width=True, hide_index=True)

    divider()

    col1, col2 = st.columns(2)
    with col1:
        section_title("Inventory")
        inv = query(f"""
            SELECT make as Make, status as Status,
                   COUNT(*) as Count,
                   ROUND(AVG(list_price), 0) as Avg_Price
            FROM main_silver.stg_vehicles
            WHERE location_id = '{location_id}'
            {"AND make = '" + f['make'] + "'" if f.get('make') else ""}
            GROUP BY make, status ORDER BY make
        """)
        st.dataframe(inv, use_container_width=True, hide_index=True)

    with col2:
        section_title("Service Center")
        svc = query(f"""
            SELECT e.full_name as Technician,
                   COUNT(j.job_id) as Jobs,
                   ROUND(COALESCE(SUM(j.labor_revenue), 0), 0) as Revenue,
                   ROUND(AVG(j.efficiency_ratio), 2) as Efficiency
            FROM main_silver.stg_employees e
            LEFT JOIN main_silver.stg_service_jobs j
                ON e.employee_id = j.technician_id
                AND j.job_date_only >= '{f['start_date']}'
                AND j.job_date_only <= '{f['end_date']}'
            WHERE e.location_id = '{location_id}'
            AND e.is_salesperson = FALSE
            GROUP BY e.employee_id, e.full_name
            ORDER BY Revenue DESC
        """)
        st.dataframe(svc, use_container_width=True, hide_index=True)

    divider()
    col1, col2, col3 = st.columns([4, 1, 4])
    with col2:
        if st.button("Sign Out", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.user = None
            st.rerun()

# ============================================================
# SALESPERSON
# ============================================================

def show_salesperson(user, f):
    employee_id = user["employee_id"]

    st.markdown(f"""
    <div style="padding:32px 48px 20px 48px;margin:0 -1rem">
        <p style="font-size:1.5rem;font-weight:700;color:#1A1A1A;
                  letter-spacing:-0.5px;margin:0">
            {user['name'].split()[0]}
            <span style="color:#B8860B">Performance</span>
        </p>
        <p style="font-size:12px;color:#999;margin:4px 0 0 0">
            {f['start_date'].strftime('%b %d')} –
            {f['end_date'].strftime('%b %d, %Y')}
        </p>
    </div>
    """, unsafe_allow_html=True)

    stats = query(f"""
        SELECT COUNT(*) as deals,
               COALESCE(SUM(sale_price), 0) as revenue,
               COALESCE(AVG(sale_price), 0) as avg_deal,
               COALESCE(MAX(sale_price), 0) as best_deal
        FROM main_silver.stg_sales_transactions
        WHERE employee_id = '{employee_id}'
        AND sale_date_only >= '{f['start_date']}'
        AND sale_date_only <= '{f['end_date']}'
    """)

    emp = query(f"""
        SELECT commission_rate FROM main_silver.stg_employees
        WHERE employee_id = '{employee_id}'
    """)

    if len(stats) > 0 and len(emp) > 0:
        r = stats.iloc[0]
        rate = emp.iloc[0]["commission_rate"]
        commission = r["revenue"] * rate

        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            st.markdown(kpi_card(
                "Deals Closed", str(int(r["deals"]))
            ), unsafe_allow_html=True)
        with c2:
            st.markdown(kpi_card(
                "Revenue", f"${r['revenue']:,.0f}"
            ), unsafe_allow_html=True)
        with c3:
            st.markdown(kpi_card(
                "Commission", f"${commission:,.0f}",
                f"{rate*100:.1f}% rate", GOLD
            ), unsafe_allow_html=True)
        with c4:
            st.markdown(kpi_card(
                "Avg Deal", f"${r['avg_deal']:,.0f}"
            ), unsafe_allow_html=True)
        with c5:
            st.markdown(kpi_card(
                "Best Sale", f"${r['best_deal']:,.0f}"
            ), unsafe_allow_html=True)

    divider()

    col1, col2 = st.columns([2, 3])
    with col1:
        section_title("Company Ranking")
        rank = query(f"""
            SELECT
                RANK() OVER (
                    ORDER BY SUM(s.sale_price) DESC
                ) as Rank,
                e2.full_name as Name,
                ROUND(COALESCE(SUM(s.sale_price), 0), 0) as Revenue
            FROM main_silver.stg_employees e2
            LEFT JOIN main_silver.stg_sales_transactions s
                ON e2.employee_id = s.employee_id
                AND s.sale_date_only >= '{f['start_date']}'
                AND s.sale_date_only <= '{f['end_date']}'
            WHERE e2.is_salesperson = TRUE
            GROUP BY e2.employee_id, e2.full_name
            ORDER BY Revenue DESC
            LIMIT 10
        """)
        st.dataframe(rank, use_container_width=True, hide_index=True)

    with col2:
        section_title("My Sales This Period")
        my_sales = query(f"""
            SELECT transaction_id as ID,
                   sale_date_only as Date,
                   ROUND(sale_price, 0) as Amount,
                   financing_approved as Financed
            FROM main_silver.stg_sales_transactions
            WHERE employee_id = '{employee_id}'
            AND sale_date_only >= '{f['start_date']}'
            AND sale_date_only <= '{f['end_date']}'
            ORDER BY Date DESC
        """)
        if len(my_sales) > 0:
            st.dataframe(
                my_sales, use_container_width=True, hide_index=True
            )
        else:
            st.info("No sales in this period.")

    divider()
    col1, col2, col3 = st.columns([4, 1, 4])
    with col2:
        if st.button("Sign Out", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.user = None
            st.rerun()

# ============================================================
# ROUTER
# ============================================================

if not st.session_state.logged_in:
    show_login()
else:
    user = st.session_state.user
    show_topnav(user)
    st.markdown(
        "<div style='padding:0 48px'>",
        unsafe_allow_html=True
    )
    filters = show_filters(user)
    st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

    if user["role"] == "Executive":
        show_executive(filters)
    elif user["role"] == "Branch Manager":
        show_branch_manager(user, filters)
    elif user["role"] == "Salesperson":
        show_salesperson(user, filters)

    st.markdown("</div>", unsafe_allow_html=True)