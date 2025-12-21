import streamlit as st
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from cassandra.cluster import Cluster
from minio import Minio
import io
from scipy.signal import find_peaks
from datetime import timedelta
import clickhouse_connect
from streamlit_autorefresh import st_autorefresh

FS = 100
DET_TRESHOLD = 0.8
P_THRESHOLD = 0.5
S_THRESHOLD = 0.5
MIN_DISTANCE_SEC = 0.5
MIN_DISTANCE = int(MIN_DISTANCE_SEC * FS)

# Cassandra
CASSANDRA_HOSTS = ['127.0.0.1']
KEYSPACE = 'seismic'
TABLE = 'window_predictions'

# MinIO
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "seismic"

# ClickHouse
CH_HOST = "localhost"
CH_PORT = 8123
CH_USER = "admin"
CH_PASS = "admin"
CH_TABLE = "predicted_samples"

st.set_page_config(layout="wide")
st.title("Basis Data Lanjut")

tabs = st.tabs(["Historical", "Live Stream"])

def build_figure(trace, det, p, s, time_axis):
    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
        subplot_titles=("Channel Z", "Channel N", "Channel E", "Probabilities"),
    )

    for i, ch in enumerate(["Z", "N", "E"]):
        fig.add_trace(
            go.Scatter(x=time_axis, y=trace[:, i], mode="lines", name=ch),
            row=i + 1,
            col=1,
        )

        for idx in find_peaks(p, height=P_THRESHOLD, distance=MIN_DISTANCE)[0]:
            if det[idx] >= DET_TRESHOLD:
                fig.add_vline(x=time_axis[idx], line_color="green", line_dash="dash")

        for idx in find_peaks(s, height=S_THRESHOLD, distance=MIN_DISTANCE)[0]:
            if det[idx] >= DET_TRESHOLD:
                fig.add_vline(x=time_axis[idx], line_color="red", line_dash="dash")

    fig.add_trace(go.Scatter(x=time_axis, y=det, name="Detection"), row=4, col=1)
    fig.add_trace(go.Scatter(x=time_axis, y=p, name="P prob"), row=4, col=1)
    fig.add_trace(go.Scatter(x=time_axis, y=s, name="S prob"), row=4, col=1)

    fig.update_yaxes(range=[0, 1], row=4, col=1)
    fig.update_layout(height=900, hovermode="x unified")
    return fig

with tabs[0]:
    st.header("Historical Data (Cassandra + MinIO)")

    @st.cache_resource
    def cassandra_session():
        cluster = Cluster(CASSANDRA_HOSTS)
        return cluster.connect(KEYSPACE)

    session = cassandra_session()

    @st.cache_data
    def load_keys_models():
        rows = session.execute(
            f"SELECT DISTINCT key, model_name FROM {TABLE}"
        )
        return sorted({(r.key, r.model_name) for r in rows})

    pairs = load_keys_models()

    key = st.selectbox("Station Key", sorted(set(p[0] for p in pairs)))
    model = st.selectbox("Model", sorted(p[1] for p in pairs if p[0] == key))

    @st.cache_data(show_spinner=True)
    def load_historical(key, model):
        rows = session.execute(
            f"""
            SELECT * FROM {TABLE}
            WHERE key = %s AND model_name = %s
            ORDER BY starttime ASC
            """,
            (key, model)
        )

        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        traces, dets, ps, ss = [], [], [], []
        t0 = None

        for row in rows:
            if t0 is None:
                t0 = row.starttime

            obj = minio_client.get_object(MINIO_BUCKET, row.minio_ref)
            with np.load(io.BytesIO(obj.read())) as f:
                traces.append(f["trace"])
                dets.append(f["dd"])
                ps.append(f["pp"])
                ss.append(f["ss"])

        return (
            np.concatenate(traces),
            np.concatenate(dets),
            np.concatenate(ps),
            np.concatenate(ss),
            t0,
        )

    trace, det, p, s, t0 = load_historical(key, model)

    end_time = t0 + timedelta(seconds=len(det) / FS)
    st.info(f"**Start:** {t0}  \n**End:** {end_time}")

    start = st.slider("Start sample", 0, len(det) - 1, 0)
    end = st.slider("End sample", start + 1, min(start + 6000, len(det)))

    trace = trace[start:end]
    det = det[start:end]
    p = p[start:end]
    s = s[start:end]

    time_axis = [
        t0 + timedelta(seconds=(start + i) / FS)
        for i in range(len(det))
    ]

    st.plotly_chart(build_figure(trace, det, p, s, time_axis), use_container_width=True)

with tabs[1]:
    st.header("Live Stream ClickHouse")

    refresh_ms = st.slider("Refresh interval (ms)", 500, 5000, 1000)
    st_autorefresh(interval=refresh_ms, key="ch_refresh")

    @st.cache_resource
    def ch_client():
        return clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASS,
        )

    ch = ch_client()

    query = f"""
        SELECT
            ts,
            trace_1,
            trace_2,
            trace_3,
            dd,
            pp,
            ss
        FROM {CH_TABLE}
        WHERE ts >= now() - INTERVAL 60 SECOND
        ORDER BY ts ASC
    """

    df = ch.query_df(query)

    if df.empty:
        st.warning("No live data yet")
        st.stop()

    trace = df[["trace_1", "trace_2", "trace_3"]].values
    det = df["dd"].values
    p = df["pp"].values
    s = df["ss"].values
    time_axis = df["ts"].values

    st.plotly_chart(
        build_figure(trace, det, p, s, time_axis),
        use_container_width=True
    )
