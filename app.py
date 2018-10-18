from datetime import datetime
import json
from elasticsearch import Elasticsearch

import paho.mqtt.client as mqtt

ELASTICSEARCH_HOST = "http://localhost"
ELASTICSEARCH_INDEX = "meraki_camera"
ELASTICSEARCH_DOC_TYPE = "doc"

MQTT_SERVER = "18.224.64.54"
MQTT_PORT = 1883
MQTT_TOPIC = "/merakimv/#"

#
COLLECT_RAW_DETECTIONS = False
#
COLLECT_ZONE_DETECTIONS = True
#
COLLECT_CAMERAS_SERIAL_NUMBERS = ["*"]
# array of zone id, all is *. eg ["*"]
COLLECT_ZONE_IDS = ["*"]


def collect_raw_detection(topic, payload):
    # e.g.
    # /merakimv/Q2GV-S7PZ-FGBK/raw_detections

    parameters = topic.split("/")
    serial_number = parameters[2]

    if len(payload["objects"]) == 0:
        return False

    for object in payload["objects"]:
        doc = {
            'topic': topic,
            'frame': object["frame"],
            'oid': object["frame"],
            'coordinate': {
                "x0": object["x0"],
                "x1": object["x1"],
                "y0": object["y0"],
                "y1": object["y1"],
            },
            'meraki_ts': payload["ts"],
            "camera_serial_number": serial_number,
            'timestamp': datetime.now(),
            "data_type": "zone"
        }

        post_msg_es(doc)


def collect_zone_information(topic, payload):
    ## /merakimv/Q2GV-S7PZ-FGBK/123

    parameters = topic.split("/")
    zone_id = parameters[3]
    index = [i for i, x in enumerate(COLLECT_ZONE_IDS) if x == zone_id]

    # if not wildcard or not in the zone_id list or equal to 0 (whole camera)
    if COLLECT_ZONE_IDS[0] != "*":
        if index == 0 or zone_id == "0":
            return

    doc = {
        'topic': topic,
        "count": payload,
        "camera_serial_number": parameters[2],
        'timestamp': datetime.now(),
        "data_type": "zone",
        "zone_id": zone_id
    }

    post_msg_es(doc)


def post_msg_es(doc):
    try:

        res = es.index(index=ELASTICSEARCH_INDEX, doc_type=ELASTICSEARCH_DOC_TYPE, body=doc)
        print("[ES]success push info to es : {0}".format(res))

    except Exception as e:
        print("[ES]failed to push info due to {0}".format(e))


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

    client.subscribe(MQTT_TOPIC)


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode("utf-8"))
    parameters = msg.topic.split("/")
    serial_number = parameters[2]

    # filter camera
    if COLLECT_CAMERAS_SERIAL_NUMBERS[0] != "*" or len(
            [i for i, x in enumerate(COLLECT_CAMERAS_SERIAL_NUMBERS) if x == serial_number]):
        return

    if COLLECT_RAW_DETECTIONS and msg.topic[-14:] == 'raw_detections':
        collect_raw_detection(msg.topic, payload)

    if COLLECT_ZONE_DETECTIONS and msg.topic[-14:] != 'raw_detections':
        collect_zone_information(msg.topic, payload)


if __name__ == "__main__":
    # init es
    es = Elasticsearch([ELASTICSEARCH_HOST])

    try:
        es.indices.create(index=ELASTICSEARCH_INDEX, ignore=400)
    except Exception as ex:
        print("[ES]failed to create the indices due to: \n {0}".format(ex))

    # mqtt
    try:
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message

        client.connect(MQTT_SERVER, MQTT_PORT, 60)
        client.loop_forever()
    except Exception as ex:
        print("[MQTT]failed to connect or receive msg from mqtt, due to: \n {0}".format(ex))
