import packaging.requirements
import packaging.version
import packaging.specifiers
import dill
import sys
import zmq
import requests
import dxspaces
from .exec_stream import ExecStream

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
    exec_token = None

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
    def set_exec_token(cls, token):
        cls.exec_token = token
        
    @classmethod
    def set_environment(cls, filename, usr_token=None):
        """
        Spawn a remote execution server of required environment dependencies 
        by sending the requirements.txt to the R-Exec API.
        """
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

        payload = {"requirments": requirements}
        if usr_token is not None:
            payload["token"] = usr_token
            cls.exec_token = usr_token
        response = requests.post(cls.rexec_api_url, data=payload)
        if response.status_code == 404:
            raise RuntimeError(f"R-Exec API url not found.")
        if not response.ok:
            raise RuntimeError(f"Failed to send requirement.txt to R-Exec API.")
        return response


    def __init__(self, func=None):
        if func is not None:
            self.func = func


    def _prepare_invocation(self, *args):
        """
        Prepare the remote execution request by serializing the function and its arguments,
        and send the request to the R-Exec broker;
        Return a ExecStream iterator to receive the stream of messages from the server(through broker).
        """
        if not self.exec_token:
            raise RuntimeError("Execution token not set; call set_environment or set_exec_token.")
        
        # Prepare zmq payload for remote execution request; 
        # the payload includes the serialized function, its arguments 
        # and an execution token for authentication/authorization.
        pfn = dill.dumps(self.func)
        pargs = dill.dumps(args)
        token = self.exec_token.encode("utf-8")

        # Prepare zmq socket for remote execution request; 
        # use a DEALER socket to preserve ROUTER envelope framing on the server side,
        # the server will reply with a stream of messages for each request
        zmq_context = zmq.Context()
        zmq_socket = zmq_context.socket(zmq.DEALER)
        zmq_addr = "tcp://" + self.broker_addr + ":" + self.broker_port

        zmq_socket.connect(zmq_addr)

        # Send the remote execution request to the R-Exec broker;
        # Preserve ROUTER envelope framing by adding explicit delimiter.
        zmq_socket.send_multipart([b"", token, pfn, pargs])
        return ExecStream(zmq_context, zmq_socket, zmq_addr, token)


    def __call__(self, *args):
        return_data = None
        has_return = False
        exec_stream_iter = self._prepare_invocation(*args)
        try:
            for event in exec_stream_iter:
                # Handle non-dict messages
                # (e.g., a simple string message from the broker)
                if not isinstance(event, dict):
                    if isinstance(event, str):
                        sys.stdout.write(event)
                        if not event.endswith("\n"):
                            sys.stdout.write("\n")
                        sys.stdout.flush()
                    else:
                        return_data = event
                        has_return = True
                    continue
                # Handle special event types for remote stdout stream;
                # print stdout events from the remote execution server to local stdout in real time
                if event.get("channel") == "stdout":
                    sys.stdout.write(event.get("data", ""))
                    sys.stdout.flush()
                # Handle stderr stream events; for example:
                # capture error messages from the remote execution and print them to local stderr
                elif event.get("channel") == "stderr":
                    sys.stderr.write(event.get("data", ""))
                    sys.stderr.flush()
                # Handle a special event type for remote execution return value
                # capture the remote execution return val and save it to return_data local var
                elif event.get("channel") == "return":
                    return_data = event.get("data")
                    has_return = True
                else:
                    continue
        # Handle KeyboardInterrupt in the client side
        except KeyboardInterrupt:
            exec_stream_iter.keyboard_interrupt()
            raise
        finally:
            exec_stream_iter.close()
        # If the remote_func has a return value,
        # return it to caller
        if has_return: return return_data
