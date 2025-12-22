import threading
import time
import pandas as pd
import logging

from source.components import InferenceSensor, IngestSensor, process_raw_trace, process_window, prediction_emitter, window_emitter, start_scylla_sink
from source.infra import ObspyClient, start_ingest, bootstrap_infra
from source.models.settings import AppSettings

def setup_logging():
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

def main():
    setup_logging()
    settings = AppSettings.from_env()
    if 60 % settings.window_size != 0:
        print("window size is need to be dividan of 60 seconds")
        return

    try:
        infra = bootstrap_infra(settings)
    except Exception as e:
        return

    ingestSensors: dict[str, IngestSensor] = {}
    inferenceSensors: dict[str, InferenceSensor] = {}
    df = pd.read_csv(settings.station_list_path)
    threads = []
    for _, row in df.iterrows():
        network = row["network"]
        station = row["station"]
        sensor_code = row["sensor_code"]          # XX
        orientation = row["orientation_order"]    # e.g. ZNE
        fs = int(row["sampling_rate"])

        if not isinstance(orientation, str) and len(orientation) != 3:
            raise ValueError(
                f"{network}.{station}.{sensor_code} invalid orientation_order: {orientation}, correct example : ZNE"
            )

        sensor_key = f"{network}.{station}.{sensor_code}"
        ingestSensor = IngestSensor(
            key=sensor_key,
            network=network,
            station=station,
            fs=fs,
            sensor_code=sensor_code,
            orientation=orientation,
            window_seconds=settings.window_size
        )
        ingestSensors[sensor_key] = ingestSensor

        inferenceSensor = InferenceSensor(
            key = sensor_key,
            network=network,
            station=station,
            fs=fs,
            sensor_code=sensor_code,
            orientation=orientation,
            window_seconds=settings.window_size
        )
        inferenceSensors[sensor_key] = inferenceSensor

        print(f"created sensor {sensor_key} ({orientation} {fs}hz)")

        for o in orientation:
            seed = sensor_code + o
            obspy_client = ObspyClient(settings.seedlink_endpoint, process_raw_trace, ingestSensor)
            seed_t = threading.Thread(
                target=start_ingest,
                args=(obspy_client, network, station, seed,),
                daemon=True
            )
            seed_t.start()
            threads.append(seed_t)

        w_emitter = threading.Thread(
            target=window_emitter,
            args=(ingestSensor,infra.producer, settings.window_seismic_topic),
            daemon=True
        )
        w_emitter.start()
        threads.append(w_emitter)

        predictor = threading.Thread(
            target=prediction_emitter,
            args=(inferenceSensor, infra.producer, infra.eqt, settings.prediction_seismic_topic),
            daemon=True
        )
        predictor.start()
        threads.append(predictor)

    process_window_t = threading.Thread(
        target=process_window,
        args=(inferenceSensors, infra.window_consumer),
        daemon=True
    )
    process_window_t.start()
    threads.append(process_window_t)

    start_scylla_sink(infra.prediction_consumer_scylla, infra.scylla)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Caught KeyboardInterrupt, stopping…")
    finally:
        print("Terminating processes…")
        infra.close()

if __name__ == "__main__":
    main()

