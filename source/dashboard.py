from datetime import datetime, time, timedelta
import pandas as pd
import streamlit as st
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from cassandra.cluster import Cluster
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

# Cassandra
CASSANDRA_HOSTS = ['127.0.0.1']
KEYSPACE = 'seismic'
TABLE = 'predicted_samples'

# ClickHouse
CH_HOST = "localhost"
CH_PORT = 8123
CH_USER = "admin"
CH_PASS = "admin"
CH_TABLE = "predicted_samples"

st.set_page_config(layout="wide")
st.title("Seismic")

tabs = st.tabs(["Live Stream", "Archieve"])

def plot_waveform_plotly(
    trace_win,
    det_win,
    p_win,
    s_win,
    time_axis, 
    P_THRESHOLD=0.5,
    S_THRESHOLD=0.5,
):

    time_axis = pd.to_datetime(time_axis)

    p_peaks, _ = find_peaks(np.nan_to_num(p_win), height=P_THRESHOLD)
    s_peaks, _ = find_peaks(np.nan_to_num(s_win), height=S_THRESHOLD)

    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        subplot_titles=[
            "Channel Z",
            "Channel N",
            "Channel E",
            "Probabilities",
        ],
    )

    channel_titles = ["Z", "N", "E"]

    for i in range(3):
        fig.add_trace(
            go.Scatter(
                x=time_axis,
                y=trace_win[:, i],
                mode="lines",
                name=f"Channel {channel_titles[i]}",
                line=dict(width=1),
            ),
            row=i + 1,
            col=1,
        )

        # P picks
        for idx in p_peaks:
            fig.add_vline(
                x=time_axis[idx],
                line_color="green",
                line_dash="dash",
                line_width=1,
                row=i + 1,
                col=1,
            )

        # S picks
        for idx in s_peaks:
            fig.add_vline(
                x=time_axis[idx],
                line_color="red",
                line_dash="dash",
                line_width=1,
                row=i + 1,
                col=1,
            )

    fig.add_trace(
        go.Scatter(x=time_axis, y=det_win, mode="lines", name="Detection"),
        row=4,
        col=1,
    )
    fig.add_trace(
        go.Scatter(x=time_axis, y=p_win, mode="lines", name="P probability"),
        row=4,
        col=1,
    )
    fig.add_trace(
        go.Scatter(x=time_axis, y=s_win, mode="lines", name="S probability"),
        row=4,
        col=1,
    )

    fig.update_yaxes(
        range=[0, 1],
        title_text="Probability",
        row=4,
        col=1,
    )

    fig.update_xaxes(title_text="Time", row=4, col=1)

    fig.update_layout(
        height=900,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=40, r=40, t=60, b=40),
    )

    st.plotly_chart(fig, use_container_width=True)

with tabs[0]:
    st.header("Live Stream (ClickHouse)")

    FS = 100
    FREQ = "10ms"
    WINDOW_SECONDS = 60
    MAX_SAMPLES = FS * WINDOW_SECONDS  # 6000 samples
    DECIMATE = 1 

    st_autorefresh(interval=10000, key="ch_refresh")

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
            orientation_order,
            trace1,
            trace2,
            trace3,
            dd,
            pp,
            ss
        FROM {CH_TABLE}
        FINAL
        WHERE ts >= now() - INTERVAL 4 MINUTE
        ORDER BY ts ASC
    """

    df = ch.query_df(query)

    if df.empty:
        st.warning("No live data yet")
        st.stop()

    df["ts"] = pd.to_datetime(df["ts"])
    df = df.set_index("ts").sort_index()

    end_ts = df.index.max()
    start_ts = end_ts - pd.Timedelta(seconds=WINDOW_SECONDS)

    full_index = pd.date_range(
        start=start_ts,
        end=end_ts,
        freq=FREQ,
    )

    df_win = df.reindex(full_index)

    cols = ["trace1", "trace2", "trace3", "dd", "pp", "ss"]
    valid_mask = df_win[cols].notna().any(axis=1)

    if valid_mask.any():
        last_valid_ts = valid_mask[valid_mask].index[-1]
        df_win = df_win.loc[:last_valid_ts]
    else:
        st.warning("No valid samples in window yet")
        st.stop()

    if len(df_win) > MAX_SAMPLES:
        df_win = df_win.iloc[-MAX_SAMPLES:]

    trace_np = df_win[["trace1", "trace2", "trace3"]].to_numpy()
    det_np   = df_win["dd"].to_numpy()
    p_np     = df_win["pp"].to_numpy()
    s_np     = df_win["ss"].to_numpy()

    if DECIMATE > 1:
        trace_np = trace_np[::DECIMATE]
        det_np   = det_np[::DECIMATE]
        p_np     = p_np[::DECIMATE]
        s_np     = s_np[::DECIMATE]

    if len(trace_np) > 1:
        plot_waveform_plotly(
            trace_np,
            det_np,
            p_np,
            s_np,
            time_axis=df_win.index,
            P_THRESHOLD=P_THRESHOLD,
            S_THRESHOLD=S_THRESHOLD,
        )


with tabs[1]:
    st.header("Archieve Data (Scylla)")

    @st.cache_resource
    def cassandra_session():
        cluster = Cluster(CASSANDRA_HOSTS)
        return cluster.connect(KEYSPACE)

    session = cassandra_session()

    rows = session.execute(
        f"SELECT DISTINCT key, model_name FROM predicted_sample_bucket_by_day"
    )
    pairs = sorted({(r.key, r.model_name) for r in rows})

    pair_labels = {
        f"{k} | {m}": (k, m)
        for k, m in pairs
    }

    selected_label = st.selectbox(
        "Station | Model",
        sorted(pair_labels.keys())
    )

    selected_key, selected_model = pair_labels[selected_label]
    day_rows = session.execute(
    """
        SELECT day
        FROM predicted_sample_bucket_by_day
        WHERE key = %s AND model_name = %s
        """,
        (selected_key, selected_model),
    )

    available_days = [r.day for r in day_rows]

    selected_day = st.selectbox(
        "Available Day",
        sorted(available_days)
    )


    @st.cache_data(ttl=60, show_spinner=True)  
    def load_historical(key, model, day):
        rows = session.execute(
            f"""
            SELECT ts, orientation_order, trace1, trace2, trace3, dd, pp, ss
            FROM {TABLE}
            WHERE key = %s AND model_name = %s and day = %s
            ORDER BY ts ASC
            """,
            (key, model, day),
        )
        rows = list(rows)

        traces, dets, ps, ss, times = [], [], [], [], []
        for r in rows:
            traces.append([r.trace1, r.trace2, r.trace3])
            dets.append(r.dd)
            ps.append(r.pp)
            ss.append(r.ss)
            times.append(r.ts)

        return (
            np.array(traces),
            np.array(dets),
            np.array(ps),
            np.array(ss),
            np.array(times),
            rows[0].orientation_order,
        )


    trace, det, p, s, time_axis, order = load_historical(selected_key, selected_model, selected_day.date())
    df = pd.DataFrame({
        "trace1": trace[:, 0],
        "trace2": trace[:, 1],
        "trace3": trace[:, 2],
        "det": det,
        "p": p,
        "s": s,
    }, index=pd.to_datetime(time_axis))


    min_ts = pd.to_datetime(time_axis[0])
    max_ts = pd.to_datetime(time_axis[-1])

    min_time = min_ts.time()
    max_time = max_ts.time()



    def init(prefix, t):
        for k, v in zip(("h", "m", "s"), (t.hour, t.minute, t.second)):
            st.session_state.setdefault(f"{prefix}_{k}", v)

    init("start", min_time)
    init("end", max_time)
    min_seconds = 0
    max_seconds = int((max_ts - min_ts).total_seconds())
    min_abs_sec = min_ts.hour * 3600 + min_ts.minute * 60 + min_ts.second
    max_abs_sec = max_ts.hour * 3600 + max_ts.minute * 60 + max_ts.second



    def get_time(prefix):
        return time(
            st.session_state[f"{prefix}_h"],
            st.session_state[f"{prefix}_m"],
            st.session_state[f"{prefix}_s"],
        )


    def set_time(prefix, t):
        st.session_state[f"{prefix}_h"] = t.hour
        st.session_state[f"{prefix}_m"] = t.minute
        st.session_state[f"{prefix}_s"] = t.second


    def clip(t, lo, hi):
        return max(lo, min(t, hi))

    def hms_to_abs_seconds(h, m, s):
        return h * 3600 + m * 60 + s


    def abs_seconds_to_hms(sec):
        h = sec // 3600
        sec %= 3600
        m = sec // 60
        s = sec % 60
        return h, m, s




    def validate_times():
        # START
        s_sec = hms_to_abs_seconds(
            st.session_state.start_h,
            st.session_state.start_m,
            st.session_state.start_s,
        )

        # END
        e_sec = hms_to_abs_seconds(
            st.session_state.end_h,
            st.session_state.end_m,
            st.session_state.end_s,
        )

        # Clip to absolute bounds
        s_sec = max(min_abs_sec, min(s_sec, max_abs_sec))
        e_sec = max(min_abs_sec, min(e_sec, max_abs_sec))

        # Enforce end > start
        if e_sec <= s_sec:
            if s_sec >= max_abs_sec:
                s_sec = max(max_abs_sec - 1, min_abs_sec)
                e_sec = max_abs_sec
            else:
                e_sec = s_sec + 1


        # Write back (NO wraparound)
        st.session_state.start_h, st.session_state.start_m, st.session_state.start_s = abs_seconds_to_hms(s_sec)
        st.session_state.end_h, st.session_state.end_m, st.session_state.end_s = abs_seconds_to_hms(e_sec)




    col1, spacer, col2 = st.columns([1, 0.2, 1])

    with col1:
        st.markdown(f"**Start time**  \nMin: `{min_ts.strftime('%H:%M:%S')}`")

        h, m, s = st.columns(3)
        h.number_input("HH", 0, 23, key="start_h", on_change=validate_times, label_visibility="collapsed")
        m.number_input("MM", -1, 60, key="start_m", on_change=validate_times, label_visibility="collapsed")
        s.number_input("SS", -1, 60, key="start_s", on_change=validate_times, label_visibility="collapsed")


    with col2:
        st.markdown(f"**End time**  \nMax: `{max_ts.strftime('%H:%M:%S')}`")

        h, m, s = st.columns(3)
        h.number_input("HH", 0, 23, key="end_h", on_change=validate_times, label_visibility="collapsed")
        m.number_input("MM", -1, 60, key="end_m", on_change=validate_times, label_visibility="collapsed")
        s.number_input("SS", -1, 60, key="end_s", on_change=validate_times, label_visibility="collapsed")


    st.divider()
    st.write("Start:", get_time("start"))
    st.write("End:", get_time("end"))
    freq = "10ms"   # 100 Hz

    full_index = pd.date_range(
        start=df.index.min(),
        end=df.index.max(),
        freq=freq,
    )
    df = df.reindex(full_index)

    trace_np = df[["trace1", "trace2", "trace3"]].to_numpy()
    det_np   = df["det"].to_numpy()
    p_np     = df["p"].to_numpy()
    s_np     = df["s"].to_numpy()

    start_dt = datetime.combine(selected_day.date(), get_time("start"))
    end_dt   = datetime.combine(selected_day.date(), get_time("end"))

    df_win = df.loc[start_dt:end_dt]

    trace_np = df_win[["trace1", "trace2", "trace3"]].to_numpy()
    det_np   = df_win["det"].to_numpy()
    p_np     = df_win["p"].to_numpy()
    s_np     = df_win["s"].to_numpy()

    if len(df_win) > 1:
        plot_waveform_plotly(
            trace_np,
            det_np,
            p_np,
            s_np,
            time_axis=df_win.index,
            P_THRESHOLD=P_THRESHOLD,
            S_THRESHOLD=S_THRESHOLD,
        )



