import logging
from obspy.clients.seedlink.easyseedlink import EasySeedLinkClient

logger = logging.getLogger("infra")

class ObspyClient(EasySeedLinkClient):
    def __init__(self, seedlink_url, on_datas, sensor):
        super().__init__(seedlink_url) 
        self.on_datas = on_datas
        self.sensor = sensor

    def on_data(self, trace):
        self.on_datas(trace, self.sensor)


def start_ingest(client, network, station_name, seed):
    client.select_stream(network, station_name, seed)
    logger.info(f"source ingestion started: {network} {station_name} {seed}")
    client.run()