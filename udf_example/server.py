
import sys
import varint
from udf import udf_pb2
from threading import Lock, Thread
from Queue import Queue

write_lock = Lock()
read_buffer_size = 1024
max_varint_size = 5

queue = Queue()

encoder = varint.encodeVarint
decoder = varint.decodeVarint32

# Get lock and write response to stdout
def write_response(response):
    write_lock.acquire()
    try:
        data = response.SerializeToString()
        encoder(sys.stdout.write, len(data))
        sys.stdout.write(data)
    finally:
        write_lock.release()


# Read requests off stdin
def read_loop():
    data = sys.stdin.read(read_buffer_size)
    request = udf_pb2.Request()
    while len(data) > 0:
        next_pos, pos = decoder(data, 0)
        size = pos + next_pos
        l = len(data)
        if size > l:
            diff = size - l
            newdata = sys.stdin.read(diff + max_varint_size)
            if len(newdata) < diff:
                print >> sys.stderr, "Error EOF"
                break

            data += newdata
        request.ParseFromString(data[pos:pos + next_pos])
        data = data[pos+next_pos:]

        # use parsed message
        msg = request.WhichOneof("message")
        if msg == "point":
            queue.put(request.point)
        elif msg == "begin":
            queue.put(request.begin)
        elif msg == "end":
            queue.put(request.end)
        elif msg == "keepalive":
            response = udf_pb2.Response()
            response.keepalive.time = request.keepalive.time
            write_response(response)
        elif msg == "state":
            response = udf_pb2.Response()
            response.state.version = 42
            write_response(response)
        elif msg == "restore":
            response = udf_pb2.Response()
            response.restore.version = 42
            write_response(response)
        elif msg == "stop":
            break
        else:
            print >> sys.stderr, msg

    queue.put(None)

# Read data messages off queue
def process_data():
    while True:
        msg = queue.get()
        if msg is None:
            break
        print >> sys.stderr, "Received data", msg

if __name__ == '__main__':
    print >> sys.stderr, "Starting server"
    read = Thread(target=read_loop)
    read.start()

    data = Thread(target=process_data)
    data.start()

    read.join()
    data.join()

