"""This is the task which is executed by the worker"""
import _xxinterpchannels as channels
import mmap
import os

MMAP_PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")


def to_int(x: bytes) -> int:
    # Parse sign
    if x[0] == 45:  # ASCII for "-"
        sign = -1
        idx = 1
    else:
        sign = 1
        idx = 0
    # Check the position of the decimal point
    if x[idx + 1] == 46:  # ASCII for "."
        # -#.# or #.#
        # 528 == ord("0") * 11
        result = sign * ((x[idx] * 10 + x[idx + 2]) - 528)
    else:
        # -##.# or ##.#
        # 5328 == ord("0") * 111
        result = sign * ((x[idx] * 100 + x[idx + 1] * 10 + x[idx + 3]) - 5328)

    return result


def process_line(line, result):
    idx = line.find(b";")

    city = line[:idx]
    temp_float = to_int(line[idx+1:-1])

    if city in result:
        item = result[city]
        item[0] += 1
        item[1] += temp_float
        item[2] = min(item[2], temp_float)
        item[3] = max(item[3], temp_float)
    else:
        result[city] = [1, temp_float, temp_float, temp_float]


# Will get OS errors if mmap offset is not aligned to page size
def align_offset(offset, page_size):
    return (offset // page_size) * page_size


def process_chunk(file_path, start_byte, end_byte):
    offset = align_offset(start_byte, MMAP_PAGE_SIZE)
    result = {}

    with open(file_path, "rb") as file:
        length = end_byte - offset

        with mmap.mmap(
            file.fileno(), length, access=mmap.ACCESS_READ, offset=offset
        ) as mmapped_file:
            mmapped_file.seek(start_byte - offset)
            for line in iter(mmapped_file.readline, b""):
                process_line(line, result)
    return result


while True:
    msg = channels.recv(channel_id, default=None)
    if msg == "stop":
        break
    elif msg is None:
        continue
    else:
        msg = eval(msg)
        result = process_chunk(*msg)
        channels.send(main_ch, str(result))
