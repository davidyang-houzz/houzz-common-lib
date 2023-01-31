#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""StreamHist testing module.

Most tests in this module are ported/adapted from the Clojure tests developed
for BigMl's "Streaming Histograms for Clojure/Java" [1].

References
----------
[1] https://github.com/bigmlcom/histogram
"""

# Copyright © 2015 Carson Farmer <carsonfarmer@gmail.com>
# Copyright © 2013, 2014, 2015 BigML
# Licensed under the Apache License, Version 2.0


from __future__ import absolute_import
import random

from houzz.common.metric.streamhist import StreamHist
from six.moves import range


def make_normal(size):
    return [random.normalvariate(0.0, 1.0) for _ in range(size)]


def test_regression():
    random.seed(1700)
    data = make_normal(10000)
    hist1 = StreamHist(maxbins=5)
    hist2 = StreamHist(maxbins=5, weighted=True)
    # hist3 = StreamHist(maxbins=5, weighted=True)
    hist4 = StreamHist(maxbins=5)

    hist1.update(data)
    hist2.update(data)
    hist3 = hist2 + hist1
    hist4.update(list(range(10000)))

    reg = [{'count': 1176.0, 'mean': -1.622498097884402},
           {'count': 5290.0, 'mean': -0.3390892100898127},
           {'count': 3497.0, 'mean': 1.0310297400593385},
           {'count': 35.0, 'mean': 2.2157182954841126},
           {'count': 2.0, 'mean': 3.563619987633774}]
    assert hist1.to_dict()["bins"] == reg

    reg = [-1.3078228593744852, -0.6987752252688707, -0.02122914279927418,
           0.764392444856377, 1.3359954834540608]
    assert hist1.quantiles(0.1, 0.25, 0.5, 0.75, 0.9) == reg

    reg = [{'count': 579.0, 'mean': -2.017257931684027},
           {'count': 1902.0, 'mean': -1.0677091300958608},
           {'count': 3061.0, 'mean': -0.24660751313691653},
           {'count': 2986.0, 'mean': 0.5523120572161528},
           {'count': 1472.0, 'mean': 1.557598912751095}]
    assert hist2.to_dict()["bins"] == reg

    reg = [-1.364586417733853, -0.6993144358459258, 0.012419381183600253,
           0.7155847806643041, 1.39142846045978]
    assert hist2.quantiles(0.1, 0.25, 0.5, 0.75, 0.9) == reg

    reg = [{'count': 1755.0, 'mean': -1.7527351028815432},
           {'count': 1902.0, 'mean': -1.0677091300958608},
           {'count': 8351.0, 'mean': -0.3051906980106826},
           {'count': 6483.0, 'mean': 0.8105375295133331},
           {'count': 1509.0, 'mean': 1.5755221868037264}]
    assert hist3.to_dict()["bins"] == reg

    reg = [-1.325738553708784, -0.6112584986586627, -0.006670722094228365,
           0.7678339018262468, 1.2185919303926576]
    assert hist3.quantiles(0.1, 0.25, 0.5, 0.75, 0.9) == reg

    reg = [{'count': 1339.0, 'mean': 669.0},
           {'count': 2673.0, 'mean': 2675.0},
           {'count': 1338.0, 'mean': 4680.5},
           {'count': 2672.0, 'mean': 6685.5},
           {'count': 1978.0, 'mean': 9010.5}]
    assert hist4.to_dict()["bins"] == reg

    reg = [1114.7853492345948, 2541.061374142397, 5112.189357410602,
           7424.275271894938, 8997.58286262782]
    assert hist4.quantiles(0.1, 0.25, 0.5, 0.75, 0.9) == reg


def test_iris_regression():
    sepal_length = [5.1, 4.9, 4.7, 4.6, 5.0, 5.4, 4.6, 5.0, 4.4, 4.9, 5.4, 4.8,
                    4.8, 4.3, 5.8, 5.7, 5.4, 5.1, 5.7, 5.1, 5.4, 5.1, 4.6, 5.1,
                    4.8, 5.0, 5.0, 5.2, 5.2, 4.7, 4.8, 5.4, 5.2, 5.5, 4.9, 5.0,
                    5.5, 4.9, 4.4, 5.1, 5.0, 4.5, 4.4, 5.0, 5.1, 4.8, 5.1, 4.6,
                    5.3, 5.0, 7.0, 6.4, 6.9, 5.5, 6.5, 5.7, 6.3, 4.9, 6.6, 5.2,
                    5.0, 5.9, 6.0, 6.1, 5.6, 6.7, 5.6, 5.8, 6.2, 5.6, 5.9, 6.1,
                    6.3, 6.1, 6.4, 6.6, 6.8, 6.7, 6.0, 5.7, 5.5, 5.5, 5.8, 6.0,
                    5.4, 6.0, 6.7, 6.3, 5.6, 5.5, 5.5, 6.1, 5.8, 5.0, 5.6, 5.7,
                    5.7, 6.2, 5.1, 5.7, 6.3, 5.8, 7.1, 6.3, 6.5, 7.6, 4.9, 7.3,
                    6.7, 7.2, 6.5, 6.4, 6.8, 5.7, 5.8, 6.4, 6.5, 7.7, 7.7, 6.0,
                    6.9, 5.6, 7.7, 6.3, 6.7, 7.2, 6.2, 6.1, 6.4, 7.2, 7.4, 7.9,
                    6.4, 6.3, 6.1, 7.7, 6.3, 6.4, 6.0, 6.9, 6.7, 6.9, 5.8, 6.8,
                    6.7, 6.7, 6.3, 6.5, 6.2, 5.9]

    h = StreamHist(maxbins=32)
    h.update(sepal_length)

    b = [{'count': 1, 'mean': 4.3},
         {'count': 4, 'mean': 4.425000000000001},
         {'count': 4, 'mean': 4.6},
         {'count': 7, 'mean': 4.771428571428571},
         {'count': 16, 'mean': 4.9625},
         {'count': 9, 'mean': 5.1},
         {'count': 4, 'mean': 5.2},
         {'count': 1, 'mean': 5.3},
         {'count': 6, 'mean': 5.4},
         {'count': 7, 'mean': 5.5},
         {'count': 6, 'mean': 5.6},
         {'count': 8, 'mean': 5.7},
         {'count': 7, 'mean': 5.8},
         {'count': 3, 'mean': 5.9},
         {'count': 6, 'mean': 6.0},
         {'count': 6, 'mean': 6.1},
         {'count': 4, 'mean': 6.2},
         {'count': 9, 'mean': 6.3},
         {'count': 7, 'mean': 6.4},
         {'count': 5, 'mean': 6.5},
         {'count': 2, 'mean': 6.6},
         {'count': 8, 'mean': 6.7},
         {'count': 3, 'mean': 6.8},
         {'count': 4, 'mean': 6.9},
         {'count': 1, 'mean': 7.0},
         {'count': 1, 'mean': 7.1},
         {'count': 3, 'mean': 7.2},
         {'count': 1, 'mean': 7.3},
         {'count': 1, 'mean': 7.4},
         {'count': 1, 'mean': 7.6},
         {'count': 4, 'mean': 7.7},
         {'count': 1, 'mean': 7.9}]
    assert h.to_dict()["bins"] == b
