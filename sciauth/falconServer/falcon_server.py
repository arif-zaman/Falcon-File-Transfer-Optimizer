from cmath import log
import requests
import time
# import datetime
import logging as logger
from os import environ
from redis import Redis
from flask import Flask, request
from flask_restful import Resource, Api


root_dir = "/home/arif/throughput-optimization/sciauth/falconServer/"
hostname = environ.get("REDIS_HOSTNAME", "localhost")
port = environ.get("REDIS_PORT", 6379)
stream_key = "falcon-transfer:{0}"
r = Redis(hostname, port, retry_on_timeout=True)

app = Flask(__name__)
api = Api(app)


log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
# log_file = datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S") + ".log"

logger.basicConfig(
    format=log_FORMAT,
    datefmt='%m/%d/%Y %I:%M:%S %p',
    level=logger.DEBUG,
    # filename=log_file,
    # filemode="w"
    handlers=[
        # logger.FileHandler(log_file),
        logger.StreamHandler()
    ]
)


def send_event(data, topic):
    try:
        r.xadd(topic, data)

    except ConnectionError as e:
        print("ERROR REDIS CONNECTION: {}".format(e))


class Welcome(Resource):
    def get(self):
        return {'data': 'Welcome to the Token Server!'}


class Transfer(Resource):
    def post(self):
        try:
            request_data = request.get_json()
            assert "user_name" in request_data, "missing field: user_name"
            assert "user_email" in request_data, "missing field: user_email"
            assert "source_address" in request_data, "missing field: source_address"
            assert "source_dir" in request_data, "missing field: source_dir"
            assert "destination_address" in request_data, "missing field: destination_address"
            assert "destination_dir" in request_data, "missing field: destination_dir"

            source = request_data["source_address"]
            dest = request_data["destination_address"]

            headers = {
                "Content-Type": "application/json; charset=utf-8"
            }

            response = requests.post(url='http://134.197.113.70:8000/token', headers=headers, json=request_data)

            if response.status_code == 200:
                event = {
                    "message": "data transfer request",
                    "source": source,
                    "destination": dest
                }
                event["token"] = response.json()[source]
                logger.debug(event)
                send_event(event, stream_key.format(source))

                event["token"] = response.json()[dest]
                logger.debug(event)
                send_event(event, stream_key.format(dest))

                response = dict()
                response["message"] = "transfer request initiated!"
                return response, 200
            else:
                return response.json(), 400
        except Exception as e:
            return {
                "message": str(e)
            }, 400


## Resources
api.add_resource(Welcome, '/')
api.add_resource(Transfer, '/transfer')


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8001, debug=True)