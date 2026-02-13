import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest, OrderBy
from google.oauth2 import service_account
import datetime

# --- Configuración Visual ---
st.set_page_config(page_title="Tablero Loteria", layout="wide", page_icon="🎰")
st.title("📊 Tablero de Control Integrado")

# --- BARRA LATERAL (FILTROS GLOBALES) ---
st.sidebar.header("📅 Configuración de Fechas")
# Por defecto: últimos 30 días
today = datetime.date.today()
default_start = today - datetime.timedelta(days=30)

start_date = st.sidebar.date_input("Fecha Inicio", default_start)
end_date = st.sidebar.date_input("Fecha Fin", today)

if start_date > end_date:
    st.sidebar.error("⚠️ La fecha de inicio no puede ser mayor a la fecha fin.")

# --- CONEXIÓN SQL (NEON) ---
def get_db_connection():
    try:
        conf = st.secrets["postgres"]
        encoded_pass = quote_plus(conf["password"])
        connection_str = f"postgresql+psycopg2://{conf['user']}:{encoded_pass}@{conf['host']}/{conf['dbname']}?sslmode=require"
        return create_engine(connection_str)
    except Exception as e:
        st.error(f"Error DB: {e}")
        return None

# --- FUNCIONES GOOGLE ANALYTICS 4 (Con Fechas Dinámicas) ---
def get_ga4_client():
    creds_dict = dict(st.secrets["google_auth"])
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    return BetaAnalyticsDataClient(credentials=credentials)

# Función auxiliar para convertir fecha python a string GA4 ('YYYY-MM-DD')
def format_date(d):
    return d.strftime("%Y-%m-%d")

def get_ga4_kpis(property_id, start, end):
    client = get_ga4_client()
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=format_date(start), end_date=format_date(end))],
        metrics=[Metric(name="totalUsers"), Metric(name="sessions"), Metric(name="screenPageViews"), Metric(name="averageSessionDuration"), Metric(name="bounceRate")]
    )
    response = client.run_report(request)
    return response.rows[0] if response.rows else None

def get_ga4_pages_source(property_id, start, end):
    client = get_ga4_client()
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=format_date(start), end_date=format_date(end))],
        dimensions=[Dimension(name="pageTitle"), Dimension(name="sessionSource")],
        metrics=[Metric(name="totalUsers")],
        limit=20
    )
    response = client.run_report(request)
    data = [{"Título": r.dimension_values[0].value, "Fuente": r.dimension_values[1].value, "Usuarios": int(r.metric_values[0].value)} for r in response.rows]
    return pd.DataFrame(data)

def get_ga4_hourly(property_id, start, end):
    client = get_ga4_client()
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=format_date(start), end_date=format_date(end))],
        dimensions=[Dimension(name="hour")],
        metrics=[Metric(name="totalUsers")],
        order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="hour"))]
    )
    response = client.run_report(request)
    data = [{"Hora": int(r.dimension_values[0].value), "Usuarios": int(r.metric_values[0].value)} for r in response.rows]
    return pd.DataFrame(data).sort_values("Hora")

# --- LAYOUT DEL DASHBOARD ---

tab1, tab2 = st.tabs(["🗄️ Base de Datos (SQL)", "📈 Tráfico Web (GA4)"])

# === PESTAÑA 1: BASE DE DATOS (Filtrada por sidebar) ===
with tab1:
    st.header("Explorador de Registros")
    engine = get_db_connection()
    
    if engine:
        with engine.connect() as conn:
            tables_df = pd.read_sql(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"), conn)
            
            if not tables_df.empty:
                col_sel, col_info = st.columns([1, 3])
                selected_table = col_sel.selectbox("Selecciona tabla:", tables_df['table_name'], index=0)
                
                # Cargamos datos
                query = text(f'SELECT * FROM "{selected_table}"')
                df = pd.read_sql(query, conn)
                
                if 'created_at' in df.columns:
                    # Filtramos usando las variables globales start_date y end_date
                    df['created_at'] = pd.to_datetime(df['created_at'])
                    mask = (df['created_at'].dt.date >= start_date) & (df['created_at'].dt.date <= end_date)
                    df_filtered = df.loc[mask]
                    
                    col_info.info(f"Mostrando {len(df_filtered)} registros entre {start_date} y {end_date}")
                    
                    st.subheader("Evolución de Registros (Periodo Seleccionado)")
                    if not df_filtered.empty:
                        daily_counts = df_filtered.groupby(df_filtered['created_at'].dt.date).size().reset_index(name='Registros')
                        daily_counts.columns = ['Fecha', 'Registros']
                        st.bar_chart(daily_counts.set_index('Fecha'), color="#22c55e")
                    else:
                        st.warning("No hay registros en estas fechas.")
                        
                    st.dataframe(df_filtered, width='stretch')
                else:
                    st.warning("Esta tabla no tiene columna de fecha, se muestran todos los datos.")
                    st.dataframe(df, width='stretch')

# === PESTAÑA 2: ANALYTICS (Filtrada por sidebar) ===
with tab2:
    prop_id = st.secrets["analytics"]["property_id"]
    st.header(f"Rendimiento del Sitio ({start_date} al {end_date})")

    # Pasamos las fechas globales a las funciones
    row = get_ga4_kpis(prop_id, start_date, end_date)
    
    if row:
        users = int(row.metric_values[0].value)
        sessions = int(row.metric_values[1].value)
        views = int(row.metric_values[2].value)
        avg_time_sec = float(row.metric_values[3].value)
        bounce = float(row.metric_values[4].value)
        avg_time_fmt = str(datetime.timedelta(seconds=int(avg_time_sec)))
        
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total Usuarios", f"{users:,}")
        k2.metric("Sesiones", f"{sessions:,}")
        k3.metric("Vistas", f"{views:,}")
        k4.metric("Duración Media", avg_time_fmt)
        k5.metric("% Rebote", f"{bounce*100:.2f}%")
        st.divider()

    col_main, col_side = st.columns([2, 1])

    with col_main:
        st.subheader("Patrón Horario (Promedio del periodo)")
        df_hourly = get_ga4_hourly(prop_id, start_date, end_date)
        if not df_hourly.empty:
            st.area_chart(df_hourly.set_index("Hora"), color="#3b82f6") 
    
    with col_side:
        st.subheader("Top Páginas y Fuentes")
        df_pages = get_ga4_pages_source(prop_id, start_date, end_date)
        if not df_pages.empty:
            st.dataframe(
                df_pages, 
                width='stretch', 
                hide_index=True,
                column_config={"Usuarios": st.column_config.ProgressColumn("Usuarios", format="%d", min_value=0, max_value=int(df_pages["Usuarios"].max()))}
            )