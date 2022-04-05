from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key, load_pem_public_key


root_dir = "/home/arif/sciauth/tokenServer/"

def gen_private_key():
    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )

    return private_key


def save_key(pk, filename, key="private"):
    if key=="private":
        pem = pk.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        )
    else:
        pem = pk.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )

    with open(filename, 'wb') as pem_out:
        pem_out.write(pem)


def load_key(filename, key="private"):
    print(filename)
    with open(filename, 'rb') as pem_in:
        if key == "private":
            return load_pem_private_key(data=pem_in.read(), password=None, backend=default_backend())
        else:
            return load_pem_public_key(data=pem_in.read(), backend=default_backend())


if __name__ == '__main__':
    private_pk = gen_private_key()
    filename = 'privatekey.pem'
    save_key(private_pk, root_dir+filename, key="private")
    pvt_key = load_key(root_dir+filename, key="private")
    print(private_pk)
    print(pvt_key)
    # assert private_pk == pvt_key, "Private keys do not match!"

    public_pk = private_pk.public_key()
    filename = 'publickey.pem'
    save_key(public_pk, root_dir+filename, key="public")

    pub_key = load_key(root_dir+filename, key="public")
    print(public_pk)
    print(pub_key)
    # assert public_pk == pub_key