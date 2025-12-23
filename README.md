# Data Streaming Pipeline for Earthquake Predition

you can add more station to process through resources/station_list.csv you need to specify the sampling rate for each station, because the system expect fixed amount of sampling rate <br>

here is the high level architecture of the system:

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

