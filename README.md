# Data Streaming Pipeline for Earthquake Predition

This project implements an end to end data streaming pipeline for near real time earthquake detection and phase picking using machine learning.

The system is designed to operate in near real time with an end to end latency ranging from approximately 10 seconds up to several minutes, depending on hardware resources and deployment configuration. The architecture is fully streaming based and uses Kafka as the backbone between processing layers, enabling horizontal scalability and fault isolation.

The pipeline is multi threaded by design. Each seismic sensor is handled independently in both the time alignment layer and the inference layer. This allows the system to scale with the number of sensors, where adding more sensors primarily requires additional compute resources rather than architectural changes. In theory, the system can handle an unbounded number of sensors.
you can add more station to process through resources/station_list.csv you need to specify the sampling rate for each station, because the system expect fixed amount of sampling rate <br>

## high level architecture of the system:

<img width="1513" height="825" alt="Screenshot 2025-12-23 211248" src="https://github.com/user-attachments/assets/393ddcff-cc23-4a9b-bb4e-44a6e531c412" />

you can run this on your own computer in any os (i think)

## Run the data pipeline

i recomend use python 3.10 virtual enivorement because this system use tf2.9

after activate the python virtual enviorement, install requirement python package:
```
  pip install -r requirements.txt
```

run docker compose :
```
docker-compose up -d
```

run the application:
```
python -m source.main
```

yeah that it...


## Run The Dashboard
you need different virtual env here because there is dependency conflict between tensorflow 2.9 and newer version of streamlit <br>
you can use any version to be honest
after activate the python virtual enviorement, install requirement python package for dashboard:
```
  pip install -r requirements-ui.txt
```
run the application:
```
streamlit run python/dashboard.py
```

