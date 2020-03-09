"""
multi-process

Created On 9th Mar, 2020
Author: bohang.li
"""

from multiprocessing import Process, Pool

cpu_num = 10


def map_fun(arg1, arg2, kw1=None):
    print("multi-process: arg1:{0}, arg2:{1}".format(arg1, arg2))


p = Pool()
for i in range(cpu_num):
    arg1 = 1
    arg2 = 2
    p.apply_async(map_fun, args=(arg1, arg2), kwds={})
    # p = Process(target=save_face_patches, args=(arg_list, "../dataset/face_patches", 1.3, i, i))
p.close()
p.join()
