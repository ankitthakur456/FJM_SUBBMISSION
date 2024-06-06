import struct
import datetime


def get_hour():
    return datetime.datetime.now().hour


def get_shift():
    return 'A'


def word_list_to_long(val_list, big_endian=True):
    # allocate list for long int
    long_list = [None] * int(len(val_list) / 2)
    # fill registers list with register items
    for i, item in enumerate(long_list):
        if big_endian:
            long_list[i] = (val_list[i * 2] << 16) + val_list[(i * 2) + 1]
        else:
            long_list[i] = (val_list[(i * 2) + 1] << 16) + val_list[i * 2]
    # return long list
    return long_list


def decode_ieee(val_int):
    return struct.unpack("f", struct.pack("I", val_int))[0]


def f_list(values, bit=False):
    fist = []
    for f in word_list_to_long(values, bit):
        fist.append(round(decode_ieee(f), 3))
    # print(len(f_list),f_list)
    return fist
