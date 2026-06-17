import os
import logging
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, dash_table
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

# Database connection 
engine = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)

CITY_COLORS = {
    "Nairobi":"#2196F3",
    "Mombasa":"#FF5722",
    "Kisumu":"#4CAF50",
    "Nakuru":"#9C27B0",
    "Eldoret":"#FF9800",
}
WMO = {
    0:"Clear",1:"Mainly clear",2:"Partly cloudy",3:"Overcast",
    45:"Fog",48:"Rime fog",51:"Light drizzle",53:"Drizzle",55:"Dense drizzle",
    61:"Slight rain",63:"Rain",65:"Heavy rain",
    80:"Rain showers",81:"Mod. showers",82:"Heavy showers",95:"Thunderstorm",
}

def query(sql: str, params: dict = {}) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)

app = Dash(__name__, title="🌦️ Kenya Weather Dashboard")

app.layout = html.Div(
    style={"fontFamily": "Segoe UI, Arial, sans-serif", "backgroundColor": "#f5f7fa", "minHeight": "100vh"},
    children=[

        # Header
        html.Div(
            style={"background": "linear-gradient(135deg,#1a237e,#0d47a1)", "color": "white", "padding": "24px 32px"},
            children=[
                html.H1("🌦️ Kenya Weather Data Warehouse", style={"margin": 0, "fontSize": "28px"}),
                html.P("Live pipeline · Medallion Architecture · 5 Cities", style={"margin": "4px 0 0", "opacity": 0.8}),
            ]
        ),

        # Tabs
        dcc.Tabs(
            style={"margin": "16px"},
            children=[

                # ─── TAB 1: Overview ─────────────────────────────────────────
                dcc.Tab(label="Overview", children=[
                    html.Div(id="overview-cards", style={"padding": "16px"}),
                    dcc.Interval(id="refresh", interval=3600_000, n_intervals=0),  # refresh hourly
                ]),

                # ─── TAB 2: Temperature ───────────────────────────────────────
                dcc.Tab(label="Temperature", children=[
                    html.Div(style={"padding": "16px"}, children=[
                        html.Label("Select cities:"),
                        dcc.Checklist(
                            id="temp-city-filter",
                            options=[{"label": c, "value": c} for c in CITY_COLORS],
                            value=list(CITY_COLORS.keys()),
                            inline=True,
                            style={"margin": "8px 0"},
                        ),
                        dcc.Graph(id="temp-chart"),
                        dcc.Graph(id="temp-range-chart"),
                    ])
                ]),

                # ─── TAB 3: Rainfall ─────────────────────────────────────────
                dcc.Tab(label="Rainfall", children=[
                    html.Div(style={"padding": "16px"}, children=[
                        dcc.Graph(id="rainfall-bar"),
                        dcc.Graph(id="rainfall-rolling"),
                    ])
                ]),

                # ─── TAB 4: Anomalies ────────────────────────────────────────
                dcc.Tab(label="Anomalies", children=[
                    html.Div(style={"padding": "16px"}, children=[
                        html.H3("Temperature Anomalies — Last 7 Days"),
                        html.P("Readings where temperature deviated more than 2σ from the 30-day baseline."),
                        dcc.Graph(id="anomaly-scatter"),
                        html.Div(id="anomaly-table", style={"marginTop": "16px"}),
                    ])
                ]),

                # ─── TAB 5: Monthly ──────────────────────────────────────────
                dcc.Tab(label="Monthly", children=[
                    html.Div(style={"padding": "16px"}, children=[
                        dcc.Graph(id="monthly-temp"),
                        dcc.Graph(id="monthly-rain"),
                    ])
                ]),

            ]
        )
    ]
)

@app.callback(Output("overview-cards", "children"), Input("refresh", "n_intervals"))
def update_overview(_):
    df = query("""
        SELECT city, summary_date, avg_temp, min_temp, max_temp,
               total_rain, avg_humidity, avg_wind_speed, dominant_code
        FROM gold.daily_summary
        WHERE summary_date = CURRENT_DATE - 1
        ORDER BY city
    """)

    if df.empty:
        return html.P("No data yet. Run: python generation/weather_ingest.py", style={"color":"red"})

    cards = []
    for _, row in df.iterrows():
        city   = row["city"]
        color  = CITY_COLORS.get(city, "#607d8b")
        code   = int(row["dominant_code"]) if pd.notna(row["dominant_code"]) else 0
        label  = WMO.get(code, "Unknown")

        cards.append(html.Div(
            style={
                "background": "white", "borderRadius": "12px", "padding": "20px",
                "boxShadow": "0 2px 8px rgba(0,0,0,0.1)", "borderTop": f"4px solid {color}",
                "minWidth": "200px", "flex": "1",
            },
            children=[
                html.H3(city, style={"color": color, "margin": "0 0 8px"}),
                html.Div(f"{row['avg_temp']:.1f}°C", style={"fontSize":"36px","fontWeight":"bold","color":"#333"}),
                html.Div(f"↓ {row['min_temp']:.1f}°  ↑ {row['max_temp']:.1f}°", style={"color":"#666","fontSize":"13px"}),
                html.Hr(style={"border":"none","borderTop":"1px solid #eee","margin":"10px 0"}),
                html.Div([
                    html.Span(f"💧 {row['total_rain']:.1f} mm rain"),
                    html.Br(),
                    html.Span(f"💨 {row['avg_wind_speed']:.1f} m/s wind"),
                    html.Br(),
                    html.Span(f"🌤 {label}"),
                ], style={"fontSize":"13px","color":"#555","lineHeight":"1.8"}),
                html.Div(f"{row['summary_date']}", style={"marginTop":"10px","fontSize":"11px","color":"#999"}),
            ]
        ))

    return html.Div([
        html.H3("Yesterday's Conditions", style={"padding":"0 4px","color":"#333"}),
        html.Div(cards, style={"display":"flex","gap":"16px","flexWrap":"wrap"}),
    ])


# ── Callback: Temperature trend ───────────────────────────────────────────────
@app.callback(
    [Output("temp-chart", "figure"), Output("temp-range-chart", "figure")],
    Input("temp-city-filter", "value"),
)
def update_temperature(cities):
    if not cities:
        return {}, {}

    df = query("""
        SELECT city, summary_date, avg_temp, min_temp, max_temp
        FROM gold.daily_summary
        WHERE summary_date >= CURRENT_DATE - 30
          AND city = ANY(:cities)
        ORDER BY city, summary_date
    """, {"cities": cities})

    fig1 = px.line(
        df, x="summary_date", y="avg_temp", color="city",
        title="Average Daily Temperature — Last 30 Days",
        labels={"summary_date": "Date", "avg_temp": "Avg Temp (°C)", "city": "City"},
        color_discrete_map=CITY_COLORS,
    )
    fig1.update_layout(plot_bgcolor="white", paper_bgcolor="white", hovermode="x unified")

    fig2 = go.Figure()
    for city in cities:
        c_df = df[df["city"] == city]
        color = CITY_COLORS.get(city, "#607d8b")
        # Shaded band between min and max
        fig2.add_trace(go.Scatter(
            x=list(c_df["summary_date"]) + list(c_df["summary_date"])[::-1],
            y=list(c_df["max_temp"]) + list(c_df["min_temp"])[::-1],
            fill="toself", fillcolor=color, opacity=0.15,
            line=dict(color="rgba(0,0,0,0)"), name=f"{city} range", showlegend=False,
        ))
        fig2.add_trace(go.Scatter(
            x=c_df["summary_date"], y=c_df["avg_temp"],
            line=dict(color=color, width=2), name=city,
        ))
    fig2.update_layout(
        title="Temperature Range (Min–Avg–Max) — Last 30 Days",
        xaxis_title="Date", yaxis_title="Temperature (°C)",
        plot_bgcolor="white", paper_bgcolor="white", hovermode="x unified",
    )

    return fig1, fig2


# ── Callback: Rainfall charts ─────────────────────────────────────────────────
@app.callback(
    [Output("rainfall-bar", "figure"), Output("rainfall-rolling", "figure")],
    Input("refresh", "n_intervals"),
)
def update_rainfall(_):
    df = query("""
        SELECT city, summary_date, total_rain, rolling_7d_rain_mm, rolling_30d_rain_mm
        FROM gold.rainfall_trends
        WHERE summary_date >= CURRENT_DATE - 30
        ORDER BY city, summary_date
    """)

    fig1 = px.bar(
        df, x="summary_date", y="total_rain", color="city",
        title="Daily Rainfall by City — Last 30 Days",
        labels={"summary_date": "Date", "total_rain": "Rain (mm)", "city": "City"},
        color_discrete_map=CITY_COLORS, barmode="group",
    )
    fig1.update_layout(plot_bgcolor="white", paper_bgcolor="white")

    fig2 = px.line(
        df, x="summary_date", y="rolling_7d_rain_mm", color="city",
        title="7-Day Rolling Average Rainfall",
        labels={"summary_date": "Date", "rolling_7d_rain_mm": "7-Day Avg Rain (mm)", "city": "City"},
        color_discrete_map=CITY_COLORS,
    )
    fig2.update_layout(plot_bgcolor="white", paper_bgcolor="white")

    return fig1, fig2


# ── Callback: Anomaly scatter + table ────────────────────────────────────────
@app.callback(
    [Output("anomaly-scatter", "figure"), Output("anomaly-table", "children")],
    Input("refresh", "n_intervals"),
)
def update_anomalies(_):
    df = query("""
        SELECT city, recorded_at AT TIME ZONE 'Africa/Nairobi' AS local_time,
               temp_celsius, baseline_avg, deviation, is_anomaly
        FROM gold.temperature_anomalies
        WHERE detected_at >= NOW() - INTERVAL '7 days'
        ORDER BY ABS(deviation) DESC
        LIMIT 200
    """)

    if df.empty:
        return {}, html.P("No anomaly data yet.")

    df["status"] = df["is_anomaly"].map({True: "⚠️ Anomaly", False: "Normal"})

    fig = px.scatter(
        df, x="local_time", y="temp_celsius", color="status", symbol="city",
        title="Temperature Readings vs Baseline — Last 7 Days",
        labels={"local_time": "Time (EAT)", "temp_celsius": "Temperature (°C)"},
        color_discrete_map={"⚠️ Anomaly": "#f44336", "Normal": "#90CAF9"},
        hover_data=["city", "baseline_avg", "deviation"],
    )
    fig.update_layout(plot_bgcolor="white", paper_bgcolor="white")

    # Show only anomalies in the table
    anomaly_df = df[df["is_anomaly"]].copy()
    anomaly_df["local_time"] = anomaly_df["local_time"].astype(str).str[:16]

    table = dash_table.DataTable(
        data=anomaly_df[["city","local_time","temp_celsius","baseline_avg","deviation"]].to_dict("records"),
        columns=[
            {"name": "City",         "id": "city"},
            {"name": "Time (EAT)",   "id": "local_time"},
            {"name": "Temp (°C)",    "id": "temp_celsius"},
            {"name": "Baseline (°C)","id": "baseline_avg"},
            {"name": "Deviation",    "id": "deviation"},
        ],
        style_cell={"textAlign": "left", "padding": "8px"},
        style_header={"backgroundColor": "#1a237e", "color": "white", "fontWeight": "bold"},
        style_data_conditional=[
            {"if": {"filter_query": "{deviation} > 3"}, "backgroundColor": "#ffebee"},
            {"if": {"filter_query": "{deviation} < -3"}, "backgroundColor": "#e3f2fd"},
        ],
        page_size=10,
        sort_action="native",
    )

    return fig, html.Div([html.H4("Anomaly Details"), table])


# ── Callback: Monthly charts ──────────────────────────────────────────────────
@app.callback(
    [Output("monthly-temp", "figure"), Output("monthly-rain", "figure")],
    Input("refresh", "n_intervals"),
)
def update_monthly(_):
    df = query("""
        SELECT city,
               TO_DATE(year::TEXT || '-' || LPAD(month::TEXT,2,'0') || '-01', 'YYYY-MM-DD') AS period,
               avg_temp, min_temp, max_temp, total_rain, rainy_days
        FROM gold.monthly_summary
        ORDER BY city, period DESC
        LIMIT 60
    """)

    if df.empty:
        return {}, {}

    fig1 = px.bar(
        df, x="period", y="avg_temp", color="city",
        title="Monthly Average Temperature",
        labels={"period": "Month", "avg_temp": "Avg Temp (°C)", "city": "City"},
        color_discrete_map=CITY_COLORS, barmode="group",
    )
    fig1.update_layout(plot_bgcolor="white", paper_bgcolor="white")

    fig2 = px.bar(
        df, x="period", y="total_rain", color="city",
        title="Monthly Total Rainfall",
        labels={"period": "Month", "total_rain": "Total Rain (mm)", "city": "City"},
        color_discrete_map=CITY_COLORS, barmode="group",
    )
    fig2.update_layout(plot_bgcolor="white", paper_bgcolor="white")

    return fig1, fig2


if __name__ == "__main__":
    print("Starting dashboard at http://localhost:8050")
    app.run(debug=True, port=8050)
