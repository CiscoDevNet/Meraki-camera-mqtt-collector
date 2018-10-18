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
COLLECT_RAW_DETECTIONS = True
#
COLLECT_ZONE_DETECTIONS = False
# array of zone id, all is *. eg ["*"]
COLLECT_ZONE_IDS = ["*"]


def process_msg(topic, payload):
    # e.g.
    # /merakimv/Q2GV-S7PZ-FGBK/raw_detections
    # /merakimv/Q2GV-S7PZ-FGBK/123

    parameters = topic.split("/")

    if not COLLECT_RAW_DETECTIONS and parameters[2] == 'raw_detections':
        return False

    if not COLLECT_ZONE_DETECTIONS and parameters[2] != 'raw_detections':
        return False

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
            "camera_serial_number": parameters[2],
            'timestamp': datetime.now(),
        }

        if COLLECT_RAW_DETECTIONS:
            doc["collect_type"] = "raw_detections"

        if COLLECT_ZONE_DETECTIONS:

            # collect all zone information
            if COLLECT_ZONE_IDS[0] == "*":
                doc["zone_id"] = parameters[2]

            # if current zoneID in the list
            if len([i for i, x in enumerate(COLLECT_ZONE_IDS) if x == parameters[2]]):
                doc["zone_id"] = parameters[2]

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
    process_msg(msg.topic, json.loads(msg.payload.decode("utf-8")))


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
