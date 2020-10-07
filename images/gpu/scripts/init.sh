#!/bin/bash
mv /mxnet/cifar10_val.rec /data/cifar10_val.rec
mv /mxnet/cifar10_train.rec /data/cifar10_train.rec
python3 /init.py  & 
sleep 10000000000
