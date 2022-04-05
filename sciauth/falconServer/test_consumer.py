from os import environ
from redis import Redis

# stream_key = environ.get("STREAM", "jarless-1")
stream_key = "falcon-transfer:{0}".format("dtn2.cs.unr.edu")


def connect_to_redis():
    hostname = environ.get("REDIS_HOSTNAME", "localhost")
    port = environ.get("REDIS_PORT", 6379)

    r = Redis(hostname, port, retry_on_timeout=True)
    return r


def get_data(redis_connection):
    while True:
        try:
            resp = redis_connection.xread({stream_key: 0}, count=1)
            if resp:
                key, messages = resp[0]
                _, data = messages[0]
                # print(resp)
                # print(messages)
                print(data)
                redis_connection.delete(key)

        except ConnectionError as e:
            print("Redis: {}".format(e))


if __name__ == "__main__":
    connection = connect_to_redis()
    get_data(connection)
