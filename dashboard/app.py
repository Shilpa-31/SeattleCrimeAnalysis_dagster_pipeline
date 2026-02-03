import streamlit as st
import pandas as pd
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import numpy as np
import folium
import geopandas as gpd
from folium.features import GeoJsonTooltip
from streamlit_folium import st_folium


# Page Configuration
st.set_page_config(
    page_title="Seattle Crime Dashboard",
    layout="wide"
)

st.markdown(
    """
    <h1 style="text-align:center; font-size:48px;">
         Seattle Crime Dashboard
    </h1>
    """,
    unsafe_allow_html=True
)

st.markdown("🔄 *Dashboard auto-refreshes every 5 minutes..!*")

# data path
CRIME_DATA_PATH = "CrimeData_Geojson_Neighbor_Spatial_Join.csv"
NEIGHBORHOOD_GEOJSON_PATH = "2016_seattle_neighborhoods.geojson"

# Data Loaders
@st.cache_data(ttl=300)
def load_crime_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["offense_date"] = pd.to_datetime(
        df["offense_date"], errors="coerce"
    )
    return df.dropna(subset=["offense_date"])


@st.cache_data
def load_neighborhoods(path: str) -> gpd.GeoDataFrame:
    return gpd.read_file(path)


crime_df = load_crime_data(CRIME_DATA_PATH)
neighborhoods_gdf = load_neighborhoods(NEIGHBORHOOD_GEOJSON_PATH)

# Inject CSS for tab alignment
st.markdown(
    """
    <style>
    div[data-baseweb="tab-list"] {
        display: flex !important;
        justify-content: center; 
        gap: 125px;               /* space between the tabs */
    }

    /* Center text inside each tab */
    div[data-baseweb="tab"] {
        text-align: center;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Tabs Name
tab1, tab2 = st.tabs([
    "📊 **CRIME ANALYTICS**",
    "🗺️ **CRIME MAP (Heat + Points)**"
])

# Sidebar Utility
def multiselect_with_select_all(
    label: str,
    options: list,
    key: str,
    placeholder: str = "Select options"
):
    select_all_key = f"{key}_select_all"
    multiselect_key = f"{key}_multiselect"

    select_all = st.sidebar.checkbox(
        f"Select All {label}",
        key=select_all_key
    )

    if select_all:
        st.sidebar.multiselect(
            label,
            options=options,
            default=options,
            key=multiselect_key,
            disabled=True
        )
        return options

    return st.sidebar.multiselect(
        label,
        options=options,
        placeholder=placeholder,
        key=multiselect_key
    )

# TAB 1 — Crime Analytics
with tab1:

    # ---------------------------
    # Sidebar Filters
    # ---------------------------
    st.sidebar.header("||  FILTERS  ||")
    st.sidebar.markdown("<br>", unsafe_allow_html=True)
    st.sidebar.markdown("<br>", unsafe_allow_html=True)

    df = crime_df.copy()

    # 1️. Year Filter
    available_years = sorted(df["offense_date"].dt.year.unique())
    selected_years = multiselect_with_select_all(
        label="Year",
        options=available_years,
        key="year"
        )
    
    df = df[df["offense_date"].dt.year.isin(selected_years)]

    # 2. Month Filter
    available_months = sorted(df["offense_date"].dt.month.unique())

    selected_months = multiselect_with_select_all(
        label="Month",
        options=available_months,
        key="month"
        )

    df = df[df["offense_date"].dt.month.isin(selected_months)]

    # 3️. Day Filter
    available_days = sorted(df["offense_date"].dt.day.unique())
    selected_days = multiselect_with_select_all(
        label="Day",
        options=available_days,
        key="day"
    )

    df = df[df["offense_date"].dt.day.isin(selected_days)]

    # 4️. Neighborhood Filter
    if "neighborhood" not in df.columns:
        st.error("Neighborhood column missing from dataset.")
        st.stop()

    neighborhoods = sorted(
        [n for n in df["neighborhood"].unique() if isinstance(n, str)]
    )
    selected_neighborhoods = multiselect_with_select_all(
        label="Neighborhood",
        options=neighborhoods,
        key="neighborhood"
    )
    df = df[df["neighborhood"].isin(selected_neighborhoods)]

    # 5. Offense Sub Category Filter
    ofc = sorted(
        [n for n in df["offense_sub_category"].unique() if isinstance(n, str)]
    )
    selected_ofc = multiselect_with_select_all(
        label="Offense Sub Category",
        options=ofc,
        key="offense_sub_category"
    )
    df = df[df["offense_sub_category"].isin(selected_ofc)]

    # 6. Precinct Filter
    precinct = sorted(
        [n for n in df["precinct"].unique() if isinstance(n, str)]
    )
    selected_precinct = multiselect_with_select_all(
        label="Precinct",
        options=precinct,
        key="precinct"
    )
    df = df[df["precinct"].isin(selected_precinct)]


    # Safety Check
    if df.empty:
        st.warning("No data available for selected filters.")
        st.stop()

    # Summary Section
    # st.subheader(f"Crime Records of Selected Year")
    st.metric("Total Crime Records of Selected Year: ", len(df))


    # Helper Function
    def get_viridis_colors(data):
        return px.colors.sample_colorscale("Viridis", np.linspace(0, 1, len(data)))


    col1, col2 = st.columns(2)

    with col1:
        # 1. Pie Chart: Crime Against Category
        crime_category_counts = df['crime_against_category'].dropna()
        if not crime_category_counts.empty:
            crime_category_counts = crime_category_counts.value_counts()
            fig_pie = px.pie(
                names=crime_category_counts.index,
                values=crime_category_counts.values,
                title="||  Crime Against Category Distribution  ||",
                hole=0.3
            )
            fig_pie.update_layout(title_x=0.1) 
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.warning("No data available for selected filters.")

    with col2:
        # 2. Pie Chart: Offense Category
        offense_category_counts = df['offense_category'].dropna()
        if not offense_category_counts.empty:
            offense_category_counts = offense_category_counts.value_counts()
            fig_offense_pie = px.pie(
                names=offense_category_counts.index,
                values=offense_category_counts.values,
                title="||  Offense Category Distribution  ||",
                hole=0.3
            )
            fig_offense_pie.update_layout(title_x=0.3)
            st.plotly_chart(fig_offense_pie, use_container_width=True)
        else:
            st.warning("No data available for selected filters.")

    st.divider()

    # 3. Crime per day 
    # ------------------
    # Datetime normalization
    df["report_date_time"] = pd.to_datetime(df["report_date_time"], errors="coerce")

    # Filter dataset (2025 → present)
    df = df[
        (df["offense_date"].dt.year >= 2025) &
        (df["report_date_time"].dt.year >= 2025)
    ]

    # Helper function: daily aggregation
    def prepare_daily_trend(df, date_col: str) -> pd.DataFrame:
        return (
            df.dropna(subset=[date_col])
            .groupby(df[date_col].dt.date)
            .size()
            .reset_index(name="count")
            .rename(columns={date_col: "date"})
            .assign(
                rolling_avg=lambda x: x["count"].rolling(
                    window=7, min_periods=1
                ).mean()
            )
        )

    offense_daily = prepare_daily_trend(df, "offense_date")
    report_daily = prepare_daily_trend(df, "report_date_time")

    # Dynamic x-axis range
    x_min = min(offense_daily["date"].min(), report_daily["date"].min())
    x_max = max(offense_daily["date"].max(), report_daily["date"].max())

    # Plotly Area Chart
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=offense_daily["date"],
        y=offense_daily["rolling_avg"],
        name="Offense Date (7-day avg)",
        mode="lines",
        fill="tozeroy",
        hovertemplate="Date: %{x}<br>Total Crime: %{y:.0f}<extra></extra>"
    ))

    fig.add_trace(go.Scatter(
        x=report_daily["date"],
        y=report_daily["rolling_avg"],
        name="Report Date (7-day avg)",
        mode="lines",
        fill="tozeroy",
        line=dict(dash="dash"),
        hovertemplate="Date: %{x}<br>Reported Crime: %{y:.0f}<extra></extra>"
    ))

    # Layout styling
    fig.update_layout(
        title=dict(
            text="||  Crime Trends: Offense Vs Report Date  ||",
            x=0.4,
            font=dict(size=16)
        ),
        xaxis=dict(
            title="Date",
            range=[x_min, x_max]
        ),
        yaxis=dict(
            title="Number of Crimes"
        ),
        hovermode="x unified",
        template="plotly_white",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=0.5,
            xanchor="center",
            x=0.5
        ),
        margin=dict(l=40, r=40, t=70, b=40),
    )
    st.plotly_chart(fig, use_container_width=True)
    st.divider()

    # 4. Crime Report Delay Distribution
    # ----------------------------------
    # Compute report delay
    df["report_delay_days"] = (df["report_date_time"] - df["offense_date"]).dt.days

    # Filter valid delays between 0 and 100
    df_delay = df[(df["report_delay_days"] >= 0) & (df["report_delay_days"] <= 60)]

    # Histogram
    fig = px.histogram(
        df_delay,
        x="report_delay_days",
        nbins=40,
        opacity=0.75,
        labels={"report_delay_days": "Report Delay (Days)"},
        title="||  Crime Report Delay Distribution  ||",
        template="plotly_white",
    )

    # Density line (KDE-style)
    hist, bin_edges = np.histogram(df_delay["report_delay_days"], bins=40, density=True)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2

    fig.add_trace(go.Scatter(
        x=bin_centers,
        y=hist * len(df_delay) * (bin_edges[1] - bin_edges[0]),
        mode="lines",
        name="Density Trend",
        line=dict(width=3),
        hovertemplate="Delay: %{x:.1f} days<extra></extra>"
    ))
    # Mean & Median reference lines
    mean_delay = df_delay["report_delay_days"].mean()
    median_delay = df_delay["report_delay_days"].median()

    fig.add_vline(
        x=mean_delay,
        line_dash="dash",
        line_color="green",
        annotation_text=f"Mean: {mean_delay:.1f} days",
        annotation_position="top",
        annotation=dict(
            y=1.01,         # slightly above the plot (1 is top)
            yref="paper",   # relative to paper (plot area)
            yanchor="bottom",
            showarrow=False,
            font=dict(color="green", size=15),
            xanchor="left"        
        )
    )

    fig.add_vline(
        x=median_delay,
        line_dash="dot",
        line_color="blue",
        annotation_text=f"Median: {median_delay:.0f} days",
        annotation_position="top",
        annotation=dict(
            y=1.01,      
            yref="paper",
            yanchor="bottom",
            showarrow=False,
            font=dict(color="blue", size=15),
            xanchor="right"
        )
    )

    # Final styling
    fig.update_layout(
        hovermode="x unified",
        title_x=0.4,
        bargap=0.05,
        margin=dict(l=40, r=40, t=60, b=40),
    )
    st.plotly_chart(fig, use_container_width=True)
    st.divider()

    # 5. Area Chart: Crime Trend Over Time
    # ------------------------------------
    # Group by day and crime category
    area_df = (
        df.groupby([df['offense_date'].dt.date, 'crime_against_category'])
        .size()
        .reset_index(name='count')
    )

    fig_area = px.area(
        area_df,
        x='offense_date',
        y='count',
        color='crime_against_category',
        title="||   Crime Trends Over Time  ||",
    )
    fig_area.update_layout(title_x=0.4, xaxis_title="Date", yaxis_title="Number of Crimes", height=500)
    st.plotly_chart(fig_area, use_container_width=True)
    st.divider()

    # 6. Scatter plot: Crime Frtequency by Time of day
    time_df = df.copy()
    time_df["hour"] = time_df["offense_date"].dt.hour

    hourly_crime = (
        time_df
        .groupby(["hour", "offense_sub_category"])
        .size()
        .reset_index(name="crime_count")
    )

    fig_time = px.scatter(
        hourly_crime,
        x="hour",
        y="crime_count",
        color="offense_sub_category",
        size="crime_count",
        title="||  Time of Day vs Crime Frequency  ||",
        size_max=30
    )
    fig_time.update_layout(
        title_x=0.3,
        xaxis_title="Hour of Day (0-23)",
        yaxis_title="Crime Count",
        height=550
    )
    st.plotly_chart(fig_time, use_container_width=True)
    st.divider()

    # 7. Bubble Chart: Crime category by neighborhood
    bubble_df = (
        df
        .groupby(["NEIGHBO", "crime_against_category"])
        .size()
        .reset_index(name="crime_count")
    )
    fig_bubble = px.scatter(
        bubble_df,
        x="NEIGHBO",
        y="crime_against_category",
        size="crime_count",
        color="crime_against_category",
        title="||  Crime Category Vs Neighborhood Distribution  ||",
        size_max=40
    )
    fig_bubble.update_layout(
        title_x=0.3,
        xaxis_title="Neighborhood",
        yaxis_title="Crime Category",
        height=600
    )
    st.plotly_chart(fig_bubble, use_container_width=True)
    st.divider()

    # 8. Offense Sub Category Bar Chart
    # ---------------------------------
    offense_code_counts = df['offense_sub_category'].dropna()
    if not offense_code_counts.empty:
        offense_sub_data = df['offense_sub_category'].value_counts().sort_index()
        offense_sub_colors = get_viridis_colors(offense_sub_data)
        fig2 = go.Figure(go.Bar(
            x=offense_sub_data.index,
            y=offense_sub_data.values,
            marker=dict(color=offense_sub_colors)
        ))
        fig2.update_layout(
            title="||   Offense Sub Category Distribution   ||",
            xaxis_title="Offense Sub Category",
            yaxis_title="Count",
            height=500,
            width=900
        )
        fig2.update_layout(title_x=0.4)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.warning("No data available for selected filters.")  
    st.divider()

    # 9. Offense Code Description Bar Chart
    # -------------------------------------
    offense_code_counts = df['offense_code_description'].dropna()
    if not offense_code_counts.empty:
        offense_code_data = df['offense_code_description'].value_counts().sort_index()
        offense_code_colors = get_viridis_colors(offense_code_data)
        fig3 = go.Figure(go.Bar(
            x=offense_code_data.index,
            y=offense_code_data.values,
            marker=dict(color=offense_code_colors)
        ))
        fig3.update_layout(
            title="||   Offense Code Description Distribution   ||",
            xaxis_title="Offense Code Description",
            yaxis_title="Count",
            height=500,
            width=900
        )
        fig3.update_layout(title_x=0.4)
        st.plotly_chart(fig3, use_container_width=True)
    else:
            st.warning("No data available for selected filters.")  
    st.divider()

    col1, col2 = st.columns(2)

    # 10. Crime Reports by Neighborhood Bar Chart
    # -------------------------------------------
    with col1:
        crime_report_counts = df.groupby('NEIGHBO').size().reset_index(name='report_number')
        if not crime_report_counts.empty:
            fig_bar_neigh = px.bar(
                crime_report_counts,
                x='NEIGHBO',
                y='report_number',
                color='report_number',
                title="||    Crime Reports by Neighborhood   ||",
                labels={'NEIGHBO': 'Neighborhoods', 'report_number': 'Number of Reports'},
                color_continuous_scale='Cividis'
            )
            fig_bar_neigh.update_layout(title_x=0.2, height=500, width=1000)
            st.plotly_chart(fig_bar_neigh, use_container_width=True)
        else:
            st.warning("No data available for selected filters.")

    # 11. Crime Reports by CRA_NAM Bar Chart
    # --------------------------------------
    with col2:
        crime_report_cra_counts = df.groupby('CRA_NAM').size().reset_index(name='report_number')
        if not crime_report_cra_counts.empty:
            fig_bar_cra = px.bar(
                crime_report_cra_counts,
                x='CRA_NAM',
                y='report_number',
                color='report_number',
                title="||  Crime Reports by Community Reporting Area Name  ||",
                labels={'CRA_NAM': 'CRA_NAM (Community Reporting Area Name)', 'report_number': 'Number of Reports'},
                color_continuous_scale='Magma'
            )
            fig_bar_cra.update_layout(title_x=0.1, height=500, width=1000)
            st.plotly_chart(fig_bar_cra, use_container_width=True)
        else:
            st.warning("No data available for selected filters.")
    st.divider()

    # Top 10 Neighborhoods Table
    # --------------------------
    st.subheader("Selected Neighborhoods by Crime Data")
    crime_report_counts = df.groupby('neighborhood').size().reset_index(name='report_number')
    st.dataframe(crime_report_counts.sort_values('report_number', ascending=False).head(15))

    # Export Filtered Data
    st.download_button(
        label="📥 Export Filtered Crime Data",
        data=df.to_csv(index=False),
        file_name="filtered_crime_data_record.csv",
        mime="text/csv"
    )
with tab2:

    # Tab 2: Crime Map (Heat + Points)
    st.header("CRIME MAP (Heat + Points)")

    # Choropleth data preparation
    crime_counts_all = (
        crime_df.groupby("NEIGHBO")
        .size()
        .reset_index(name="report_number")
    )

    # Merge with neighborhood GeoDataFrame
    neighborhoods_map = neighborhoods_gdf.merge(
        crime_counts_all,
        on="NEIGHBO",
        how="left"
    )
    neighborhoods_map["report_number"] = neighborhoods_map["report_number"].fillna(0)

    # Initialize Folium map
    m = folium.Map(
        location=[47.6097, -122.3331],
        zoom_start=12,
        tiles="CartoDB positron"
    )

    # Choropleth Layer
    folium.Choropleth(
        geo_data=neighborhoods_map,
        data=neighborhoods_map,
        columns=["NEIGHBO", "report_number"],
        key_on="feature.properties.NEIGHBO",
        fill_color="YlOrRd",
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name="Total Reported Crime Count"
    ).add_to(m)

    # Neighborhood Tooltips
    folium.GeoJson(
        neighborhoods_map,
        style_function=lambda _: {
            "fillColor": "transparent",
            "color": "black",
            "weight": 0.5
        },
        tooltip=GeoJsonTooltip(
            fields=["NEIGHBO", "report_number"],
            aliases=["Neighborhood:", "Total Crimes:"],
            localize=True,
            sticky=True
        )
    ).add_to(m)

    # Define a color mapping for each crime category
    crime_colors = {
        "ALL OTHER": "blue",
        "PROPERTY CRIME": "red",
        "VIOLENT CRIME": "green",
    }

    # Crime Points with dynamic colors
    for _, row in df.iterrows():
        if pd.notna(row["latitude"]) and pd.notna(row["longitude"]):
            # Get color from mapping, default to gray if not found
            color = crime_colors.get(row["offense_category"], "gray")
            
            folium.CircleMarker(
                location=[row["latitude"], row["longitude"]],
                radius=3,
                color=color,
                fill=True,
                fill_opacity=0.6,
                popup=f"""
                    <b>Geojson Neighborhood:</b> {row['NEIGHBO']}<br>
                    <b>Crime Category:</b> {row['offense_category']}<br>
                    <b>Crime Description:</b> {row['offense_code_description']}<br>
                    <b>CrimeData Neighborhood:</b> {row['neighborhood']}<br>
                """
            ).add_to(m)        

    # Render Folium map in Streamlit
    st_folium(m, width=900, height=650)
