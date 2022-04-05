from os import environ
from redis import Redis

stream_key = environ.get("STREAM", "")

def connect_to_redis():
    hostname = environ.get("REDIS_HOSTNAME", "localhost")
    port = environ.get("REDIS_PORT", 6379)

    r = Redis(hostname, port, retry_on_timeout=True)
    return r


if __name__ == "__main__":
    connection = connect_to_redis()
    data = {
        "test": 1,
    }

    try:
        resp = connection.xadd(stream_key, data)
        print(resp)

    except ConnectionError as e:
        print("Redis: {}".format(e))
