import logging
import pandas as pd
import argparse
import json
import time
import threading
from tb_gateway_mqtt import TBDeviceMqttClient
from tb_device_mqtt import TBPublishInfo
import influxdb_client
from influxdb_client.client.warnings import MissingPivotFunction
import warnings
from tb_rest_client.rest_client_pe import RestClientPE
from dataclasses import dataclass
import numpy
from datetimehandler import DateTimeHandler
from dotenv import load_dotenv
import os

load_dotenv()
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=MissingPivotFunction)

# Get TB creds
TB_ACCESS_TOKENS = {
    "101": os.getenv("101_ACCESS_TOKEN"),
    "107": os.getenv("107_ACCESS_TOKEN"),
}
TB_SERVER = os.getenv("TB_SERVER")
TB_PORT = 8883
TB_ALARM_DEVICE_TOKEN = os.getenv("TB_ALARM_DEVICE_TOKEN")


# Get influx creds
INFLUX_METRICS_BUCKET = os.getenv("INFLUX_METRICS_BUCKET")
INFLUX_LOCATION_BUCKET = os.getenv("INFLUX_LOCATION_BUCKET")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_URL = os.getenv("INFLUX_URL")

# Default Tenant Administrator credentials
username = os.getenv("TENANT_USERNAME")
password = os.getenv("TENANT_PASSWORD")


logging.basicConfig(level=logging.DEBUG)

influx_client = None
args_ = None
fields = [
    "time",
    "latitude",
    "longitude",
    "altitude_ellipsoid",
    "num_svs",
    "num_sv",
    "velocity_x",
    "velocity_y",
    "velocity_z",
    "rtk_status",
]


@dataclass
class QueryResults:
    telemetry: dict


class ThingsboardInterface:
    def __init__(self, username, password):
        self.url = "https://thingsboard.cloud"
        self.username = username
        self.password = password
        self.rest_client = self.initialize_rest_client()

    def initialize_rest_client(self):
        rest_client = RestClientPE(base_url=self.url)
        rest_client.login(username=self.username, password=self.password)
        return rest_client


def get_value_from_df(df: pd.DataFrame, measurement: str, field: str):
    try:
        val = df[df["_measurement"] == measurement][field].values[0]
    except (KeyError, IndexError):
        return None
    return val



## Function to get data from influx cloud
def replay_cloud_data(
    host_info: dict, query_handler: DateTimeHandler = None
) -> pd.DataFrame:
    query_handler.range = 60
    start, stop = query_handler.get_query_start_stop()

    print(start, stop)
    if (query_handler.step_time() == 1) and not False:
        return 2

    cfg = {
        "org": "humatics_focus",
        "data_bucket": "agg_data",
        "measurement": "MTi-680G",
    }
    query_api = influx_client.query_api()

    ## __--------------------- OLD QUERY __------------------------
    # query = f"""
    #     import "math"
    #     data_MTi680 = from(bucket: "{cfg['data_bucket']}")
    #         |> range(start: {start}, stop: {stop})
    #         |> filter(fn: (r) => r["_measurement"] == "MTi-680G" """

    # if host_info is not None:
    #     for host_info_field in host_info:
    #         host_info_value = host_info[host_info_field]
    #         query += f''' and r["{host_info_field}"] == "{host_info_value}"'''
    # query += ")\n"
    # query += f"""|> filter(fn: (r) =>
    #             r["_field"] == "latitude" or
    #             r["_field"] == "longitude" or
    #             r["_field"] == "altitude_ellipsoid" or
    #             r["_field"] == "num_svs" or
    #             r["_field"] == "num_sv" or
    #             r["_field"] == "rtk_status" or
    #             r["_field"] == "velocity_x" or
    #             r["_field"] == "velocity_y" or
    #             r["_field"] == "velocity_z"
    #         )
    #         |> fill(usePrevious: true)
    #         velocity = data_MTi680
    #             |> filter(fn: (r) => r["_field"] == "velocity_x" or r["_field"] == "velocity_y" or r["_field"] == "velocity_z")
    #             |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    #             |> drop(columns: ["_time"])
    #             |> map(fn: (r) => ({{r with _time: r._time, "speed": math.sqrt(x: float(v: r.velocity_x^2.0 + r.velocity_y^2.0 + r.velocity_z^2.0))}}))
    #             |> keep(columns: ["_measurement", "speed"])

    #         speed = velocity
    #             |> last(column: "speed")

    #         max_speed = velocity
    #             |> rename(columns: {{speed: "max_speed"}})
    #             |> max(column: "max_speed")

    #         avg_speed = velocity
    #             |> rename(columns: {{speed: "avg_speed"}})
    #             |> mean(column: "avg_speed")

    #         distance = avg_speed
    #             |> map(fn: (r) => ({{ r with distance: r.avg_speed * {query_handler._step * (1.0)} }}))
    #             |> drop(columns: ["avg_speed"])

    #         outGoing = data_MTi680
    #             |> aggregateWindow(every: 10m, fn: last, createEmpty: false)
    #             |> keep(columns: ["_measurement", "_field", "_time", "_value"])
    #             |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    #             |> sort(columns: ["_time"])
    #             |> group(columns: ["_time"], mode:"by")

    #         j1 = join(tables: {{avg_speed: avg_speed, max_speed: max_speed}}, on: ["_measurement"])
    #         j2 = join(tables: {{distance: distance, speed: speed}}, on: ["_measurement"])
    #         j3 = join(tables: {{j1: j1, j2: j2}}, on: ["_measurement"])
    #         j4 = join(tables: {{j3: j3, outGoing: outGoing}}, on: ["_measurement"])

    #         j4 |> yield(name: "mqttData")"""

    # ---------------------- New Query ------------------------
    query = f"""
        import "math"
        from(bucket: "{cfg['data_bucket']}")
            |> range(start: {start}, stop: {stop})
            |> filter(fn: (r) => r["_measurement"] == "MTi-680G" """

    if host_info is not None:
        for host_info_field in host_info:
            host_info_value = host_info[host_info_field]
            query += f''' and r["{host_info_field}"] == "{host_info_value}"'''
    query += ")\n"
    query += f"""|> filter(fn: (r) =>
                r["_field"] == "latitude" or
                r["_field"] == "longitude" or
                r["_field"] == "altitude_ellipsoid" or
                r["_field"] == "num_svs" or
                r["_field"] == "num_sv" or
                r["_field"] == "rtk_status" or
                r["_field"] == "velocity_x" or
                r["_field"] == "velocity_y" or
                r["_field"] == "velocity_z"
            )
            |> duplicate(column: "_stop", as: "_time")
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> map(fn: (r) => ({{r with
                speed: math.sqrt(x: float(v: r.velocity_x^2.0 + r.velocity_y^2.0 + r.velocity_z^2.0))
            }}))
            |> map(fn: (r) => ({{r with
                distance: r.speed * {query_handler._step * 1.0},
                
            }}))
            |> group(columns: ["_time"], mode: "by")
            |> drop(columns: ["_start", "_stop", "host", "line", "operator", "train"])
            |> yield(name: "mqttData")"""

    # print(query)

    try:
        result = query_api.query_data_frame(query, org=INFLUX_ORG)
    except Exception as e:
        logging.warning(f"Location data query failed: {e}")
        return None
    if result.empty:
        logging.warning("Empty query: skipping")
        return None

    # logging.info(f"Query result: \n{result}")
    telemetry = {}
    for key in result:
        val = get_value_from_df(result, "MTi-680G", key)
        if not (isinstance(val, (numpy.int64, numpy.datetime64))):
            telemetry.update({key: get_value_from_df(result, "MTi-680G", key)})

    query = f"""
        maxLat = from(bucket: "{cfg['data_bucket']}")
            |> range(start: {start}, stop: {stop})
            |> filter(fn: (r) => r["_measurement"] == "MTi-680G" """
    if host_info is not None:
        for host_info_field in host_info:
            host_info_value = host_info[host_info_field]
            query += f''' and r["{host_info_field}"] == "{host_info_value}"'''
    query += ")\n"

    query += f"""|> filter(fn: (r) => r["_field"] == "linear_acceleration_x")
            |> max()
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])
            |> group(columns: ["_time"], mode:"by")
            |> rename(columns: {{linear_acceleration_x: "max_lateral_accel"}})
            |> yield()"""

    try:
        result = query_api.query_data_frame(org=INFLUX_ORG, query=query)
    except Exception as e:
        logging.warning(f"Acceleration data query failed: {e}")
        return None
    if result.empty:
        logging.warning("Empty query: skipping")
        return None

    # logging.info(f"Query result: \n{result}")
    for key in result:
        val = get_value_from_df(result, "MTi-680G", key)
        if not (isinstance(val, (numpy.int64, numpy.datetime64))):
            telemetry.update({key: get_value_from_df(result, "MTi-680G", key)})

    # logging.info(f"Telemetry: {telemetry}")

    telemetry["host"] = "demo"
    telemetry["operator"] = "demo"
    telemetry["line"] = "demo"
    telemetry["train"] = "Sim 101" if host_info["train"] == "101" else "Sim 107"


    print(telemetry["speed"], telemetry["latitude"], telemetry["latitude"])

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(telemetry, f, ensure_ascii=False, indent=4)

    return QueryResults(telemetry=telemetry)


def publish_results(
    client: TBDeviceMqttClient, results_message: dict, isAlram: bool = False
):

    # Skip publishing empty query results
    if len(results_message) == 0 or None in results_message.items():
        return
    try:
        result = client.send_telemetry(results_message)
        if result.get() == TBPublishInfo.TB_ERR_SUCCESS:
            if isAlram:
                logging.info("Alarm Sent! Disconneting from client")
                client.disconnect()
            else:
                logging.info(f"Published telemetry to {results_message['train']} ")
    except Exception as e:
        logging.exception(f" Failed to publish to Thingsboard: {e}")


def execute_query(
    start: str,
    duration: str,
    tb_client,
    host_info: dict,
    seconds_between_queries: int,
    trip_num,
):
    query_handler = DateTimeHandler(60, start, duration, seconds_between_queries)
    rows = []
    while True:
        results = replay_cloud_data(host_info, query_handler)
        if results is not None:
            if results == 2:
                break
            rows.append(results.telemetry)
            publish_results(tb_client, results.telemetry)

        ## Adjust sleep based on animation (duration in seconds) - 1  in widget code.
        ## Duration 1000 = time.sleep(0). Duration 10000 = time.sleep(9).  
        # time.sleep(1)
    if trip_num:
        name = "_".join(host_info.values()) +'_trip_'+ trip_num + ".parquet"
        df = pd.DataFrame.from_dict(rows)
        df.to_parquet(name)

## Get Data from cloud 
def toCloud(start, duration, operator: str, line: str, train_num: str, trip_number):
    global influx_client, args_
    # args_ = input_params

    host_info = {"operator": operator, "line": line, "train": train_num}

    # Setup Thingsboard and InfluxDB clients
    influx_client = influxdb_client.InfluxDBClient(
        org=INFLUX_ORG, url=INFLUX_URL, token=INFLUX_TOKEN
    )
    tb_client = TBDeviceMqttClient(
        TB_SERVER, TB_PORT, TB_ACCESS_TOKENS[train_num], quality_of_service=0
    )
    tb_client.connect(tls=True, ca_certs="thingsboard/ca-root.pem", keepalive=60)
    execute_query(start, duration, None, host_info, 1, trip_number)



## Old code. May not be needed anymore 
def parse_args():
    argParser = argparse.ArgumentParser()
    argParser.add_argument(
        "-s",
        "--Start",
        help="Start time of query: 2023-12-01T12:01:02.000Z",
        default="2024-07-23T19:36:25.000000Z",
        nargs="?",
    )
    argParser.add_argument(
        "-d",
        "--Duration",
        help="Duration after start time to replay data: 1:30:00",
        default="01:30:00",
        nargs="?",
    )
    argParser.add_argument(
        "-l",
        "--Loop",
        help="If flag is set, replay will loop after completion",
        action="store_false",
    )

    return argParser.parse_args()


def sim_rtd():
    ## Load rtd_location data per second
    loc_data = pd.read_parquet("rtd_sim.parquet")

    ## Create TB client for RTD Device
    client = TBDeviceMqttClient(
        TB_SERVER, TB_PORT, "JOCAx4sLqukyDW3UlTnG", quality_of_service=0
    )
    client.connect(tls=True, ca_certs="thingsboard/ca-root.pem", keepalive=60)

    telemetry = {
        "line": "d-line",
        "operator": "rtd",
        "train": "RTD D-line",
        "stationing": 0,
    }

    while True:
        for i in range(0, len(loc_data), 10):
            # print(loc_data.iloc[i]['lat'] , loc_data.iloc[i]['long'] )
            telemetry["latitude"] = loc_data.iloc[i]["lat"]
            telemetry["longitude"] = loc_data.iloc[i]["long"]
            telemetry["speed"] = loc_data.iloc[i]["speed"]
            publish_results(client, telemetry)
            time.sleep(10)



import datetime


def train_101():
    ## Load rtd_location data per second
    trip_1 = pd.read_parquet("njt_nlr_101_trip1_with_stationing_YH.parquet")
    trip_2 = pd.read_parquet("njt_nlr_101_trip2_with_stationing_YH.parquet")

    ## Create TB client for RTD Device
    client = TBDeviceMqttClient(
        TB_SERVER, TB_PORT, TB_ACCESS_TOKENS["101"], quality_of_service=0
    )
    client.connect(tls=True, ca_certs="thingsboard/ca-root.pem", keepalive=60)

    telemetry = {
        "line": "demo",
        "operator": "demo",
        "host": "demo",
        "train": "Sim 101",
        "stationing": 0,
    }

    while True:
        # First Part of the trip
        for i in range(0, len(trip_1)):
            telemetry["latitude"] = trip_1.iloc[i]["latitude"]
            telemetry["longitude"] = trip_1.iloc[i]["longitude"]
            telemetry["speed"] = trip_1.iloc[i]["speed"]
            telemetry['heading'] = 'South Bound'
            telemetry['stationing'] = trip_1.iloc[i]['atp'] 

            ## debug print
            # print(telemetry)

            publish_results(client, telemetry)
            time.sleep(1)

        print("Trip 1 completed ")
        time.sleep(60)                          ## Interval between two trips in seconds 
        print("Starting Trip 2")


        # ## Reverse trip
        for i in range(0, len(trip_2)):
            telemetry["latitude"] = trip_2.iloc[i]["latitude"]
            telemetry["longitude"] = trip_2.iloc[i]["longitude"]
            telemetry["speed"] = trip_2.iloc[i]["speed"]
            telemetry['heading'] = 'North Bound'
            telemetry['stationing'] = trip_2.iloc[i]['atp'] 

            # telemetry['track'] = trip_1.iloc[i]['track']
            # print(telemetry)

            publish_results(client, telemetry)
            time.sleep(1)


def train_107():
    ## Load 107 location data per second
    trip_1 = pd.read_parquet("njt_nlr_107_trip1_with_stationing_YH.parquet")
    trip_2 = pd.read_parquet("njt_nlr_107_trip2_with_stationing_YH.parquet")

    ## Create TB client for RTD Device
    client = TBDeviceMqttClient(
        TB_SERVER, TB_PORT, TB_ACCESS_TOKENS["107"], quality_of_service=0
    )
    client.connect(tls=True, ca_certs="thingsboard/ca-root.pem", keepalive=60)

    telemetry = {
        "line": "demo",
        "operator": "demo",
        "host": "demo",
        "train": "Sim 107",
        "stationing": 0,
    }

    while True:
        # Trip 1
        for i in range(0, len(trip_1)):
            telemetry["latitude"] = trip_1.iloc[i]["latitude"]
            telemetry["longitude"] = trip_1.iloc[i]["longitude"]
            telemetry["speed"] = trip_1.iloc[i]["speed"]
            telemetry['heading'] = 'North Bound'
            telemetry['stationing'] = trip_1.iloc[i]['atp'] 


            # telemetry['track'] = trip_1.iloc[i]['track']
            print(telemetry)

            publish_results(client, telemetry)
            time.sleep(1)

        print('Trip 1 completed')
        time.sleep(60)                              # interval between trips in seconds 
        print('Starting Trip 2')

        ## Trip 2
        for i in range(0, len(trip_2)):
            telemetry["latitude"] = trip_2.iloc[i]["latitude"]
            telemetry["longitude"] = trip_2.iloc[i]["longitude"]
            telemetry["speed"] = trip_2.iloc[i]["speed"]
            telemetry['heading'] = 'South Bound'
            telemetry['stationing'] = trip_2.iloc[i]['atp'] 

            # telemetry['track'] = trip_1.iloc[i]['track']
            # print(telemetry)

            publish_results(client, telemetry)
            time.sleep(1)


if __name__ == "__main__":
    # args_ = parse_args()

    ## Gather data for specific train and timestamp
    # sim_101 = threading.Thread(
    #     target=toCloud,
    #     args=("2024-10-4T13:40:35.000000Z", "00:14:20", "njt", "nlr", "101", "_1_sec_interval"),
    # )
    # sim_101.start()
    # sim_101.join()

    ''' 1 - {
            101 : trip from Grove to Norfolk (South Bound)
            107 : trip from Norfolk to Grove  (North Bound)
        2 - {
            101: trip from Norfolk to Grove (North Bound)
            107  : trip from Grove to Norfolk (South Bound)
        }
    }'''
    timestamps = {
        1: {
            101: {"start": "2024-10-4T13:10:55.000000Z", "duration": "00:15:25"},
            107: {"start": "2024-09-18T12:10:50.000000", "duration": "00:15:00"},
        },
        2: {
            101: {"start": "2024-10-4T13:40:50.000000Z", "duration": "00:14:20"},
            107: {"start": "2024-10-1T11:04:00.000000Z", "duration": "00:14:50"},
        },
    }


    ## Gather data for all trains 
    # for trip_number, trains in timestamps.items():
    #     trip_threads = []
    #     for train_number, data in trains.items():
    #         thread = threading.Thread(
    #             target=toCloud,
    #             args=(
    #                 data["start"],
    #                 data["duration"],
    #                 "njt",
    #                 "nlr",
    #                 str(train_number),
    #                 str(trip_number),
    #             ),
    #         )
    #         trip_threads.append(thread)
    #         thread.start()
    #     for thread in trip_threads:
    #         thread.join()
    #     print(f"Trip {trip_number} finished ")
    #     time.sleep(20)



    threading.Thread(target=train_101).start()
    threading.Thread(target=train_107).start()
    # threading.Thread(target=sim_rtd).start()
