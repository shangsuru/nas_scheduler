import argparse
import logging
import mxnet as mx
from common import find_mxnet, data, fit
from common.util import download_file


logging.basicConfig(filename="/data/training.log", filemode="w", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler())


if __name__ == "__main__":
    # parse args
    parser = argparse.ArgumentParser(
        description="train imagenet", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    fit.add_fit_args(parser)
    data.add_data_args(parser)
    data.add_data_aug_args(parser)

    # use a large aug level
    data.set_data_aug_level(parser, 3)
    parser.set_defaults(
        # network
        network="resnet",
        num_layers=50,
        # data
        num_classes=1000,
        num_examples=1281167,
        image_shape="3,224,224",
        min_random_scale=1,  # if input image has min size k, suggest to use
        # 256.0/x, e.g. 0.533 for 480
        # train
        num_epochs=80,
        lr_step_epochs="30,60",
    )
    args = parser.parse_args()

    # load network
    from importlib import import_module

    net = import_module("symbols." + args.network)
    sym = net.get_symbol(**vars(args))

    # train
    fit.fit(args, sym, data.get_rec_iter)
