import jwt
import packaging.requirements
import packaging.version
import packaging.specifiers
import dill
import sys
import zmq
import requests
import dxspaces

def parse_requirements(filename):
    with open(filename, 'r') as fd:
        for line in fd:
            req_str = line.strip()
            if not req_str or req_str == '' or req_str.startswith('#'):
                continue
            else:
                req = packaging.requirements.Requirement(req_str)
                if (req.name == "python" and req.specifier.__len__() == 1 and
                    packaging.specifiers.Specifier(req.specifier.__str__()).operator == "=="):
                    python_version = packaging.specifiers.Specifier(req.specifier.__str__()).version
                    


class remote_func:
    broker_addr = None
    broker_port = "5559"
    rexec_api_url = None

    @classmethod
    def set_remote_addr(cls, addr):
        cls.broker_addr = addr
    
    @classmethod
    def set_remote_port(cls, port):
        cls.broker_port = port

    @classmethod
    def set_api_url(cls, url):
        cls.rexec_api_url = url
        
    @classmethod
    def set_environment(cls, filename, usr_token=None):
        requirements = []
        with open(filename, 'r') as fd:
            for line in fd:
                req_str = line.strip()
                if not req_str or req_str == '' or req_str.startswith('#'):
                    continue
                else:
                    try:
                        req = packaging.requirements.Requirement(req_str)
                    except packaging.requirements.InvalidRequirement:
                        print(f"{req_str} does not conform to the specification of dependency specifiers!")
                        raise
                    else:
                        if (req.name == "python" and (req.specifier.__len__() != 1 or
                            packaging.specifiers.Specifier(req.specifier.__str__()).operator != "==")):
                            raise packaging.requirements.InvalidRequirement(f"We only support an exact Python version specification. For example: python==3.13")
                        requirements.append(req_str)
        if usr_token:
            jwks_url = "https://idp-test.nationaldataplatform.org/realms/NDP/protocol/openid-connect/certs"
            jwks_client = jwt.PyJWKClient(jwks_url)
            signing_key = jwks_client.get_signing_key_from_jwt(usr_token)
            decoded = jwt.decode(
                usr_token,
                signing_key.key,            # RSA public key
                algorithms=["RS256"],
                audience="account",         # matches the "aud" claim in the token
                issuer="https://idp-test.nationaldataplatform.org/realms/NDP"
            )
            usr_id = decoded["sub"]
        else:
            raise RuntimeError("Token is required to set up the R-Exec environment.")

        response = requests.post(cls.rexec_api_url, data={"requirments": requirements, "user_id": usr_id})
        if response.status_code == 404:
            raise RuntimeError(f"R-Exec API url not found.")
        if not response.ok:
            raise RuntimeError(f"Failed to send requirement.txt to R-Exec API.")

    def __init__(self, func=None):
        if func is not None:
            self.func = func

    def __call__(self, *args):
        pfn = dill.dumps(self.func)
        pargs = dill.dumps(args)

        zmq_context = zmq.Context()
        zmq_socket = zmq_context.socket(zmq.REQ)
        zmq_addr = "tcp://" + self.broker_addr + ":" + self.broker_port

        zmq_socket.connect(zmq_addr)

        zmq_mp_msg = [pfn, pargs]
        zmq_socket.send_multipart(zmq_mp_msg)

        zmq_ret_msg = zmq_socket.recv()
        ret_msg = dill.loads(zmq_ret_msg)

        zmq_socket.disconnect(zmq_addr)
        zmq_socket.close()
        zmq_context.destroy()

        return ret_msg