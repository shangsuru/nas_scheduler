import os
import argparse
import logging

logging.basicConfig(filename="/data/training.log", filemode="w", level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

from common import data, fit
from math import ceil
import mxnet as mx


if __name__ == "__main__":
    train_fname = "cifar10_val.rec"
    val_fname = "cifar10_train.rec"

    # parse args
    parser = argparse.ArgumentParser(
        description="train cifar10", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    fit.add_fit_args(parser)
    data.add_data_args(parser)
    data.add_data_aug_args(parser)
    data.set_data_aug_level(parser, 2)

    num_examples = 50000
    batch_size = 64
    disp_batches = 10
    parser.set_defaults(
        # network
        network="resnet",
        num_layers=50,
        # data
        data_train=train_fname,
        data_val=val_fname,
        num_classes=10,
        num_examples=num_examples,
        image_shape="3,28,28",
        pad_size=4,
        # train
        batch_size=batch_size,
        disp_batches=disp_batches,
        num_epochs=300,
        lr=0.05,
        lr_step_epochs="200,250",
    )
    args = parser.parse_args()

    # load network
    from importlib import import_module

    net = import_module("symbols." + args.network)  # import the network
    sym = net.get_symbol(**vars(args))  # get the symbolic graph

    # train_log = '/home/net/mxnet/example/image-classification/logs/cifar10/'
    # os.system("rm -r " + train_log + "*")
    # batch_end_callbacks = [tensormetrics.LogMetricsCallback(train_log, num_examples, batch_size, disp_batches)]
    # train
    fit.fit(args, sym, data.get_rec_iter)
