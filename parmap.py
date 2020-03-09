"""
multi-process using parmap

Created On 9th Mar, 2020
Author: bohang.li
"""
import parmap


def fun(arg):
    print(arg)


arg = 1
cpu_num = 10
res = parmap.map(fun, arg, pm_processes=cpu_num, pm_pbar=True)
