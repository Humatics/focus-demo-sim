import logging
import pandas as pd
import argparse
import random
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


warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=MissingPivotFunction)


# Thingboard Variables
TB_ACCESS_TOKENS = {"101": "DizKqRfajWFk8d9ei2Et", "107": "sdhcrt37hvvb074t25mj"}
TB_SERVER = "mqtt.thingsboard.cloud"
TB_PORT = 8883
TB_ALARM_DEVICE_TOKEN = "23pGa0husleuYWSZIB6a"

# Influx Variables
INFLUX_METRICS_BUCKET = "agg_system_metrics_downsampled"
INFLUX_LOCATION_BUCKET = "agg_data"
INFLUX_ORG = "humatics_focus"
INFLUX_TOKEN = "80qC-r4muraROg-Lc_joUimIW1CJjeLUnO3KqBpMVm_DzEyFyWX1nW-MJWKO4D2ulMnXhbNoh-97Co50Rc392Q=="
INFLUX_URL = "https://us-east-1-1.aws.cloud2.influxdata.com"

# Default Tenant Administrator credentials
username = "macos@test.com"
password = "test@12345"


logging.basicConfig(level=logging.DEBUG)

exit_event = threading.Event()
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
alarmSent = []


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


def replay_cloud_data(
    host_info: dict, query_handler: DateTimeHandler = None
) -> pd.DataFrame:
    query_handler.range = 60
    start, stop = query_handler.get_query_start_stop()

    if (query_handler.step_time() == 1) and not args_.Loop:
        return None

    cfg = {
        "org": "humatics_focus",
        "data_bucket": "agg_data",
        "measurement": "MTi-680G",
    }
    query_api = influx_client.query_api()

    query = f"""
        import "math"
        data_MTi680 = from(bucket: "{cfg['data_bucket']}")
            |> range(start: {start}, stop: {stop})
            |> filter(fn: (r) => r["_measurement"] == "MTi-680G" """

    if host_info is not None:
        for host_info_field in host_info:
            host_info_value = host_info[host_info_field]
            query += f''' and r["{host_info_field}"] == "{host_info_value}"'''
    query += ")\n"

    query += f"""|> filter(fn: (r) =>
                r["_field"] == "latitude" or r["_field"] == "longitude" or r["_field"] == "altitude_ellipsoid" or
                r["_field"] == "num_svs" or r["_field"] == "num_sv" or r["_field"] == "rtk_status" or
                r["_field"] == "velocity_x" or r["_field"] == "velocity_y" or r["_field"] == "velocity_z")
            |> fill(usePrevious: true)

            velocity = data_MTi680
                |> filter(fn: (r) => r["_field"] == "velocity_x" or r["_field"] == "velocity_y" or r["_field"] == "velocity_z")
                |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> drop(columns: ["_time"])
                |> map(fn: (r) => ({{r with _time: r._time, "speed": math.sqrt(x: float(v: r.velocity_x^2.0 + r.velocity_y^2.0 + r.velocity_z^2.0))}}))
                |> keep(columns: ["_measurement", "speed"])

            speed = velocity
                |> last(column: "speed")

            max_speed = velocity
                |> rename(columns: {{speed: "max_speed"}})
                |> max(column: "max_speed")

            avg_speed = velocity
                |> rename(columns: {{speed: "avg_speed"}})
                |> mean(column: "avg_speed")

            distance = avg_speed
                |> map(fn: (r) => ({{ r with distance: r.avg_speed * {query_handler._step * (1.0)} }}))
                |> drop(columns: ["avg_speed"])

            outGoing = data_MTi680
                |> aggregateWindow(every: 10m, fn: last, createEmpty: false)
                |> keep(columns: ["_measurement", "_field", "_time", "_value"])
                |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"])
                |> group(columns: ["_time"], mode:"by")

            j1 = join(tables: {{avg_speed: avg_speed, max_speed: max_speed}}, on: ["_measurement"])
            j2 = join(tables: {{distance: distance, speed: speed}}, on: ["_measurement"])
            j3 = join(tables: {{j1: j1, j2: j2}}, on: ["_measurement"])
            j4 = join(tables: {{j3: j3, outGoing: outGoing}}, on: ["_measurement"])

            j4 |> yield(name: "mqttData")
            """

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
        logging.warning(f"Location data query failed: {e}")
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

    # with open("data.json", "w", encoding="utf-8") as f:
    #     json.dump(telemetry, f, ensure_ascii=False, indent=4)

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
        logging.warning(f" Failed to publish to Thingsboard: {e}")


def execute_query(
    tb_client: TBDeviceMqttClient, host_info: dict, seconds_between_queries: int
):
    query_handler = DateTimeHandler(
        60, args_.Start, args_.Duration, seconds_between_queries
    )
    while not tb_client.stopped:
        results = replay_cloud_data(host_info, query_handler)
        if results is not None:
            publish_results(tb_client, results.telemetry)

        time.sleep(seconds_between_queries)


def toCloud(input_params: argparse.Namespace, operator: str, line: str, train_num: str):
    global influx_client, args_
    args_ = input_params

    host_info = {"operator": operator, "line": line, "train": train_num}

    # Setup Thingsboard and InfluxDB clients
    influx_client = influxdb_client.InfluxDBClient(
        org=INFLUX_ORG, url=INFLUX_URL, token=INFLUX_TOKEN
    )
    tb_client = TBDeviceMqttClient(
        TB_SERVER, TB_PORT, TB_ACCESS_TOKENS[train_num], quality_of_service=0
    )
    tb_client.connect(tls=True, ca_certs="thingsboard/ca-root.pem", keepalive=60)
    execute_query(tb_client, host_info, 10)


def parse_args():
    argParser = argparse.ArgumentParser()
    argParser.add_argument(
        "-s",
        "--Start",
        help="Start time of query: 2023-12-01T12:01:02.000Z",
        default="2024-07-25T10:15:00.000000Z",
        nargs="?",
    )
    argParser.add_argument(
        "-d",
        "--Duration",
        help="Duration after start time to replay data: 1:30:00",
        default="00:30:00",
        nargs="?",
    )
    argParser.add_argument(
        "-l",
        "--Loop",
        help="If flag is set, replay will loop after completion",
        action="store_true",
    )

    return argParser.parse_args()


def get_alarms_fromTB(
    client: RestClientPE,
    pageSize: int = 1,
    text: str = None,
):
    res = client.get_all_alarms(page_size=pageSize, page=0, text_search=text)
    return res.data


def modify_alert(alert_to_sent, tb_queried_alert):
    ## modify alert with alert queried from TB
    alert_to_sent["lastDetection"]["location"]["latitude"] = tb_queried_alert.details[
        "latitude"
    ]
    alert_to_sent["lastDetection"]["location"]["longitude"] = tb_queried_alert.details[
        "longitude"
    ]

    alert_to_sent["summary"]["compositeAlertID"] = tb_queried_alert.details[
        "compositeAlertID"
    ]
    alert_to_sent["summary"]["customerAlertID"] = (
        "DEMO-" + tb_queried_alert.details["customerAlertID"].split("-")[1]
    )

    alert_to_sent["lastDetection"]["classification"][0]["severity"] = (
        tb_queried_alert.details["criticality"]
    )
    alert_to_sent["summary"]["firstDetected"] = tb_queried_alert.details[
        "first_detect_date"
    ]
    alert_to_sent["summary"]["lastDetected"] = tb_queried_alert.details[
        "last_detect_date"
    ]
    alert_to_sent["lastDetection"]["defectTypeName"] = tb_queried_alert.details["name"]
    alert_to_sent["lastDetection"]["trainsDetected"] = tb_queried_alert.details[
        "trains_detected_by"
    ]

    alert_to_sent["lastDetection"]["speed"]["min"] = tb_queried_alert.details[
        "min_speed"
    ]
    alert_to_sent["lastDetection"]["speed"]["avg"] = tb_queried_alert.details[
        "avg_speed"
    ]
    alert_to_sent["lastDetection"]["speed"]["max"] = tb_queried_alert.details[
        "max_speed"
    ]

    alert_to_sent["summary"]["totalDetections"] = tb_queried_alert.details[
        "detections_per_day"
    ]
    alert_to_sent["lastDetection"]["plot"] = tb_queried_alert.details["plot"]

    return alert_to_sent


def send_random_alarm():
    global alarmSent
    TB = ThingsboardInterface("testemail45@email.com", "test12345")

    ## Get previously send alarms if any
    if not alarmSent:
        logging.info("Getting alarms sent till now...")
        with open("alert_sent.txt", "r") as file:
            alarmSent = [line.rstrip("\n") for line in file]

    alarm_client = TBDeviceMqttClient(
        TB_SERVER, TB_PORT, TB_ALARM_DEVICE_TOKEN, quality_of_service=1
    )
    alarm_client.connect(tls=True, ca_certs="thingsboard/ca-root.pem", keepalive=30)

    time.sleep(1)

    ## Get random alert from NJT customer
    tb_alert = random.choice(get_alarms_fromTB(TB.rest_client, pageSize=50))

    ## Base alert details
    with open("alert.json", "r") as f:
        base = json.load(f)

    alert = modify_alert(base, tb_alert)

    ## Keep track of alarms sent
    alertID = alert["summary"]["compositeAlertID"]
    alarmSent.append(alertID)

    ## Publish alarm to TB track receiver
    publish_results(alarm_client, alert, isAlram=True)


def alarmDelete():
    global alarmSent
    TB = ThingsboardInterface("testAdmin@gmailf.wd", "test12345")

    ## Get previously send alarms if any
    if not alarmSent:
        logging.info("Getting alarms sent till now...")
        with open("alert_sent.txt", "r") as file:
            alarmSent = [line.rstrip("\n") for line in file]

    logging.info(f"Alarms sent till now - {alarmSent}")

    if not alarmSent:
        logging.info("No alerts on file")
        return

    try:
        res = get_alarms_fromTB(TB.rest_client, text=alarmSent[-1])

        if not res:
            logging.info("No Alarms to delete...")
            return

        alarmID = res[0].id.id

        logging.info(f"Deleting alarm with ID- {alarmID}")
        # First acknowledge alarm, then clear it then delete it.
        res = TB.rest_client.ack_alarm(alarm_id=alarmID)
        time.sleep(1)
        if res.acknowledged:
            res = TB.rest_client.clear_alarm(alarm_id=alarmID)

            time.sleep(1)
            if res.cleared:
                res = TB.rest_client.delete_alarm(alarm_id=alarmID)

        ## remove alertID from list
        alarmSent.pop()

    except Exception as e:
        logging.error(e)


def closing():
    ## Function to log alert sent for future use
    logging.info("Saving alert sent")
    with open("alert_sent.txt", "+w") as f:
        for id in alarmSent:
            f.write(str(id) + "\n")


if __name__ == "__main__":
    args_ = parse_args()

    with open("train_config.json", "r") as f:
        config = json.load(f)

    # Start sim trains on seperate threads
    for operator in config:
        for line in config[operator]:
            for train in config[operator][line]:
                sim_thread = threading.Thread(
                    target=toCloud, args=(args_, operator, line, train)
                )
                sim_thread.daemon = True
                sim_thread.start()

    try:
        while True:
            user_input = input("Enter command: \n")

            if user_input == "1":
                thread = threading.Thread(target=send_random_alarm)
                thread.start()
                thread.join()

            elif user_input == "2":
                thread = threading.Thread(target=alarmDelete)
                thread.start()
                thread.join()

            elif user_input == "3":
                ## Debug print
                logging.info(f"Alarms sent till now - {alarmSent}")

            elif user_input.lower() == "0":
                closing()
                break
            else:
                print("Unknown command.")
    except KeyboardInterrupt:
        closing()
