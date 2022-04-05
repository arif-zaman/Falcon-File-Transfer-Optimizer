import imp
import scitokens
# import datetime
import logging as logger
from flask import Flask, request
from flask_restful import Resource, Api
from pathlib import Path
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from tinydb import TinyDB, Query
from tinydb.operations import set
# from tinydb.storages import MemoryStorage

root_dir = "/home/arif/throughput-optimization/sciauth/tokenServer/"
path = Path(root_dir + "tokendb.json")
issuer = "https://hpcn.unr.edu"
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


def load_key(filename):
    with open(filename, 'rb') as pem_in:
        private_key = serialization.load_pem_private_key(pem_in.read(), None, default_backend())

    return private_key


private_key = load_key(filename=root_dir+"privatekey.pem")
public_key = private_key.public_key()
public_pem = public_key.public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo
)

db = TinyDB(path)
# db = TinyDB(storage=MemoryStorage)
config_tbl = db.table('configs_data')
# token_tbl = db.table('token_data')
query = Query()
app = Flask(__name__)
api = Api(app)


class Welcome(Resource):
    def get(self):
        return {'data': 'Welcome to the Token Server!'}


def prepare_scope(username, address):
    scopes = ''
    configs = config_tbl.search((query.site_address == address) & (query.user == username))
    if len(configs)>0:
        values = configs[0]["scopes"]
        for key in values:
            scopes += f"{key}:/{username}/{values[key]} "
    else:
        configs = config_tbl.search((query.site_address == address))
        if len(configs)>0:
            values = configs[0]["scopes"]
            for key in values:
                scopes += f" {key}:/{values[key]}"

    return scopes.strip()


class IssueToken(Resource):
    def post(self):
        try:
            request_data = request.get_json()
            assert "user_name" in request_data, "missing field: user_name"
            assert "user_email" in request_data, "missing field: user_email"
            assert "source_address" in request_data, "missing field: source_address"
            assert "source_dir" in request_data, "missing field: source_dir"
            assert "destination_address" in request_data, "missing field: destination_address"
            assert "destination_dir" in request_data, "missing field: destination_dir"

            user_name = request_data["user_name"]
            user_email = request_data["user_email"]
            source = request_data["source_address"]
            src_dir = request_data["source_dir"]
            dest = request_data["destination_address"]
            dest_dir = request_data["destination_dir"]

            token = scitokens.SciToken(key=private_key)
            token["user"] = user_name
            token["email"] = user_email
            token['source'] = source
            token['destination'] = dest

            ## Source Token
            scopes_src = prepare_scope(user_name, source)
            token['scope'] = scopes_src + f" read:{src_dir}"
            # token['io_dir'] = src_dir
            token.update_claims({'aud': source, "iss": issuer})
            serialized_token_src = token.serialize(issuer = issuer).decode("utf-8")

            # Destination Token
            scopes_dest = prepare_scope(user_name, dest)
            token['scope'] = scopes_dest + f" write:{dest_dir}"
            # token['io_dir'] = dest_dir
            token.update_claims({'aud': dest, "iss": issuer})
            serialized_token_dest = token.serialize(issuer = issuer).decode("utf-8")
            response = {
                source: serialized_token_src,
                dest: serialized_token_dest
            }

            # logger.debug(response)

            return response

        except Exception as e:
            return {
                "message": str(e)
            }, 400


class SetScopes(Resource):
    def post(self):
        try:
            request_data = request.get_json()
            assert "site_address" in request_data, "missing field: site_address"
            assert "user_specific" in request_data, "missing field: user_specific"
            assert "scopes" in request_data, "missing field: scopes"

            data = {
                "site_address": request_data["site_address"],
                "user": "ALL",
                "scopes": request_data["scopes"],
            }
            if request_data["user_specific"] == 1:
                assert "user" in request_data, "missing field: user"
                data["user"] = request_data["user"]

            configs = config_tbl.search((query.site_address == data["site_address"]) & (query.user == data["user"]))
            if len(configs) == 0:
                config_tbl.insert(data)
            else:
                for _ in configs:
                    config_tbl.update(
                        set("scopes", data["scopes"]),
                        (query.site_address == data["site_address"]) & (query.user == data["user"])
                    )

            limit = 10
            return {
                "scopes": config_tbl.search((query.site_address == data["site_address"]) & (query.user == data["user"]))[:limit]
            }, 201

        except Exception as e:
            return {
                "message": str(e)
            }, 400

## Resources
api.add_resource(Welcome, '/')
api.add_resource(IssueToken, '/token')
api.add_resource(SetScopes, '/configuration')


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)