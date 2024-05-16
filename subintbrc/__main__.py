import sys
import mmap
import os
import threading

import _xxinterpchannels as channels

from .worker import Worker

CPU_COUNT = os.cpu_count()
ROOT_DIR = os.path.dirname(__file__)
task_str = open(os.path.join(ROOT_DIR, "_task.py"), "r").read()


def recv(channel_id):
    results = []
    while True:
        result = channels.recv(channel_id, default=None)
        if result == "stop":
            break
        elif result is None:
            continue
        else:
            result = eval(result)
            results.append(result)
    final = reduce(results)


def reduce(results):
    final = {}
    for result in results:
        for city, item in result.items():
            if city in final:
                city_result = final[city]
                city_result[0] += item[0]
                city_result[1] += item[1]
                city_result[2] = min(city_result[2], item[2])
                city_result[3] = max(city_result[3], item[3])
            else:
                final[city] = item
    return final


def main(fpath):
    fsize_bytes = os.path.getsize(fpath)
    base_chunk_size = fsize_bytes // CPU_COUNT
    chunks = []  # send chunks through the channels

    with open(fpath, "r+b") as f:
        with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mapped:
            start_byte = 0
            for _ in range(CPU_COUNT):
                end_byte = min(start_byte + base_chunk_size, fsize_bytes)
                end_byte = mapped.find(b"\n", end_byte)
                end_byte = end_byte + 1 if end_byte != -1 else fsize_bytes
                chunks.append((fpath, start_byte, end_byte))
                start_byte = end_byte

    threads = []
    main_ch = channels.create()

    for i in range(CPU_COUNT):
        t = Worker(task_str, main_ch)
        t.start()
        threads.append(t)

    receiver = threading.Thread(target=recv, args=(main_ch,), daemon=True)
    receiver.start()

    for idx, chunk in enumerate(chunks):
        threads[idx % len(threads)].process_chunk(str(chunk))

    for t in threads:
        t.request_stop()
        t.join()

    channels.send(main_ch, "stop")
    receiver.join()


main(sys.argv[1])
