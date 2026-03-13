import dill
import zmq

STREAM_EVENT_START = "START"
STREAM_EVENT_DATA = "DATA"
STREAM_EVENT_END = "END"
STREAM_EVENT_ERROR = "ERROR"
STREAM_CANCEL_FRAME = b"__REXEC_CANCEL__"
KEYBOARD_INTERRUPT_TEXT = "keyboard_interrupt"

class ExecStream:
    def __init__(self, zmq_context, zmq_socket, zmq_addr, token=None):
        self._zmq_context = zmq_context
        self._zmq_socket = zmq_socket
        self._zmq_addr = zmq_addr
        self._token = token
        self._closed = False

    @staticmethod
    def _split_envelope(frames):
        for idx, frame in enumerate(frames):
            if frame == b"":
                envelope = frames[:idx]
                body = frames[idx + 1:]
                return envelope, idx, body
        return [], None, frames

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if self._closed:
                raise StopIteration

            # Expecting a multipart message with an optional ROUTER envelope followed by the payload;
            # where each message is either a stream event (with a dict payload containing "type" field)
            # or a regular message (with non-dict payload).
            frames = self._zmq_socket.recv_multipart()
            _envelope, delimiter_index, body = self._split_envelope(frames)
            if delimiter_index is not None:
                if not body:
                    continue
                payload = body[0]
            else:
                if not frames:
                    continue
                payload = frames[0]

            # Deserialize the payload
            message = dill.loads(payload)
            
            # If the message is not a dict, treat it as a regular message 
            # (e.g., a simple string message from the broker)
            if not isinstance(message, dict):
                self.close()
                return message
            
            # Handle stream events based on the "type" field in the message dict
            msg_type = message.get("type")
            if msg_type == STREAM_EVENT_START:
                continue
            if msg_type == STREAM_EVENT_DATA:
                return message
            if msg_type == STREAM_EVENT_END:
                self.close()
                raise StopIteration
            if msg_type == STREAM_EVENT_ERROR:
                self.close()
                raise RuntimeError(message.get("message", "Remote stream failed."))

            return message

    # Close the zmq socket and context to clean up resources
    def close(self):
        if self._closed:
            return
        self._closed = True
        self._zmq_socket.disconnect(self._zmq_addr)
        self._zmq_socket.close()
        self._zmq_context.destroy()

    # Send a control message to the server with a specific frame type and body frames;
    def _send_msg(self, frame_type, *body_frames):
        if self._closed or self._token is None:
            return False
        try:
            self._zmq_socket.send_multipart([b"", self._token, frame_type, *body_frames])
            return True
        except zmq.ZMQError:
            return False
    
    # Send a cancel control message to STOP remote execution.
    def cancel(self, text=KEYBOARD_INTERRUPT_TEXT):
        if self._closed:
            return
        payload = "" if text is None else str(text)
        # frame0:__REXEC_CANCEL__, frame1: keyboard_interrupt
        self._send_msg(STREAM_CANCEL_FRAME, payload.encode("utf-8"))

    # Handle KeyboardInterrupt in the client side 
    # by sending a cancel message to the server to stop func's remote execution child process, and then close the stream.
    def keyboard_interrupt(self):
        if self._closed:
            return
        self.cancel(KEYBOARD_INTERRUPT_TEXT)
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
