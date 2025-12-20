import math
from io import BytesIO

import streamlit as st
import numpy as np

import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy.signal import find_peaks

from cassandra.cluster import Cluster
from minio import Minio


# ================= CONFIG =================

CASSANDRA_HOSTS = ["127.0.0.1"]
KEYSPACE = "seismic_data"
TABLE = "window_predictions"

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "seismic"

DET_THRESHOLD = 0.5
P_THRESHOLD = 0.3
S_THRESHOLD = 0.3
MIN_DISTANCE_SEC = 0.5

# ================= STREAMLIT =================

st.set_page_config(layout="wide")
st.title("Seismic Streaming Dashboard")

# ================= CONNECTIONS =================

@st.cache_resource
def get_cassandra():
    return Cluster(CASSANDRA_HOSTS).connect(KEYSPACE)

@st.cache_resource
def get_minio():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

session = get_cassandra()
minio = get_minio()

# ================= METADATA =================

@st.cache_data
def get_key_model_pairs():
    rows = session.execute(
        f"SELECT key, model_name, starttime FROM {TABLE}"
    )

    latest = {}
    for r in rows:
        k = (r.key, r.model_name)
        latest[k] = max(latest.get(k, r.starttime), r.starttime)

    return sorted(latest.keys(), key=lambda x: latest[x])

# ================= DATA LOAD =================

@st.cache_data
@st.cache_data
def load_data(key, model):
    rows = session.execute(
        f"""
        SELECT starttime, sampling_rate, minio_ref
        FROM {TABLE}
        WHERE key=%s AND model_name=%s
        ORDER BY starttime ASC
        """,
        (key, model),
    )

    times = []
    traces = []
    dets = []
    ps = []
    ss = []

    fs = None
    last_end_time = None

    for r in rows:
        fs = int(r.sampling_rate)

        obj = minio.get_object(MINIO_BUCKET, r.minio_ref)
        raw = obj.read()
        obj.close()
        obj.release_conn()

        arr = np.load(BytesIO(raw))

        n = arr["dd"].shape[0]

        t0 = np.datetime64(r.starttime, "ns")
        dt = np.timedelta64(int(1e9 // fs), "ns")
        t = t0 + np.arange(n) * dt

        if last_end_time is not None:
            gap = t[0] - last_end_time
            if gap > dt:
                times.append(np.array([np.datetime64("NaT")]))
                traces.append(np.full((1, 3), np.nan))
                dets.append(np.array([np.nan]))
                ps.append(np.array([np.nan]))
                ss.append(np.array([np.nan]))

        times.append(t)
        traces.append(arr["trace"])
        dets.append(arr["dd"])
        ps.append(arr["pp"])
        ss.append(arr["ss"])

        last_end_time = t[-1] + dt

    return {
        "time": np.concatenate(times),
        "trace": np.vstack(traces),
        "det": np.concatenate(dets),
        "p": np.concatenate(ps),
        "s": np.concatenate(ss),
        "fs": fs,
    }


# ================= PLOT =================

def make_figure(data, t_start, t_end):
    mask = (data["time"] >= t_start) & (data["time"] <= t_end)

    fig = make_subplots(
        rows=4, cols=1, shared_xaxes=True,
        subplot_titles=("Z", "N", "E", "DET / P / S"),
    )

    for i in range(3):
        fig.add_trace(
            go.Scattergl(
                x=data["time"][mask],
                y=data["trace"][mask, i],
                showlegend=False,
            ),
            row=i + 1, col=1,
        )

    fig.add_trace(go.Scattergl(x=data["time"][mask], y=data["det"][mask], name="DET"), 4, 1)
    fig.add_trace(go.Scattergl(x=data["time"][mask], y=data["p"][mask], name="P"), 4, 1)
    fig.add_trace(go.Scattergl(x=data["time"][mask], y=data["s"][mask], name="S"), 4, 1)

    dist = int(MIN_DISTANCE_SEC * data["fs"])
    pks_p, _ = find_peaks(data["p"][mask], height=P_THRESHOLD, distance=dist)
    pks_s, _ = find_peaks(data["s"][mask], height=S_THRESHOLD, distance=dist)

    for i in pks_p:
        if data["det"][mask][i] >= DET_THRESHOLD:
            fig.add_vline(x=data["time"][mask][i], line_color="green", line_dash="dash")

    for i in pks_s:
        if data["det"][mask][i] >= DET_THRESHOLD:
            fig.add_vline(x=data["time"][mask][i], line_color="red", line_dash="dash")

    fig.update_yaxes(range=[0, 1], row=4, col=1)
    fig.update_layout(height=900, hovermode="x unified")

    return fig

# ================= UI (FIXED) =================

pairs = get_key_model_pairs()
labels = [f"{k} | {m}" for k, m in pairs]

choice = st.selectbox("Key / Model", labels)
key, model = pairs[labels.index(choice)]

DATA = load_data(key, model)

t_min = DATA["time"][0]
t_max = DATA["time"][-1]

# ---- PURE NUMPY TIME ----
duration_sec = int((t_max - t_min) / np.timedelta64(1, "s"))

window_sec = st.slider(
    "Window (sec)",
    min_value=10,
    max_value=min(600, duration_sec),
    value=120,
)

center_sec = st.slider(
    "Scroll",
    min_value=0,
    max_value=duration_sec,
    value=duration_sec,
)

center_time = t_min + np.timedelta64(center_sec, "s")
half = np.timedelta64(window_sec // 2, "s")

t_start = center_time - half
t_end = center_time + half

st.plotly_chart(
    make_figure(DATA, t_start, t_end),
    use_container_width=True,
)
