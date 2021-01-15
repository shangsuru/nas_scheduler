import json
import logging
import os
import redis
import sys
import time


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(module)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
ROLE = os.getenv("ROLE")
WORK_DIR = os.getenv("WORK_DIR")
JOB_NAME = os.getenv("JOB_NAME")


def update_progress(logfile):
    filesize = 0
    line_number = 0

    redis_connection = redis.Redis("130.83.143.241")
    keys = [
        "{}-progress".format(JOB_NAME),
        "{}-train_acc".format(JOB_NAME),
        "{}-train-loss".format(JOB_NAME),
        "{}-val-acc".format(JOB_NAME),
        "{}-val-loss".format(JOB_NAME),
        "{}-time-cost".format(JOB_NAME),
    ]
    for key in keys:
        redis_connection.set(key, 0)

    logging.info("starting progress monitor to track training progress ...")

    # Epoch[0] Time cost=50.885
    # Epoch[1] Batch [70]	Speed: 1.08 samples/sec	accuracy=0.000000
    epoch = 0
    batch = 0
    train_acc = {}
    train_loss = {}
    val_acc = {}
    val_loss = {}
    time_cost = {}
    while True:
        time.sleep(10)
        try:
            current_size = os.path.getsize(logfile)
        except OSError as e:
            logging.warning(e)
            continue
        if current_size == filesize:  # no changes in the log file
            continue
        else:
            filesize = current_size

        with open(logfile, "r") as f:
            for i in range(line_number):
                f.readline()
            for line in f:
                line_number += 1

                line = line.replace("\n", "")
                # only work for image-classification example
                epoch_index = line.find("Epoch")
                if epoch_index > -1:
                    epoch = int(line[line.find("[", epoch_index) + 1 : line.find("]", epoch_index)])

                    # batch
                    # TODO batches are now grouped
                    batch_index = line.find("Batch")
                    if batch_index > -1:
                        batch = int(line[line.find("[", batch_index) + 1 : line.find("-", batch_index)])
                    else:
                        batch = -1  # the end of this epoch
                    # train-acc
                    train_acc_index = line.find("Train-accuracy")
                    if train_acc_index > -1:
                        train_acc[epoch] = float(line[(line.find("=") + 1) :])

                    train_loss_index = line.find("Train-cross-entropy")
                    if train_loss_index > -1:
                        train_loss[epoch] = float(line[(line.find("=") + 1) :])

                    val_acc_index = line.find("Validation-accuracy")
                    if val_acc_index > -1:
                        val_acc[epoch] = float(line[(line.find("=") + 1) :])

                    val_loss_index = line.find("Validation-cross-entropy")
                    if val_loss_index > -1:
                        val_loss[epoch] = float(line[(line.find("=") + 1) :])

                    time_cost_index = line.find("Time cost")
                    if time_cost_index > -1:
                        time_cost[epoch] = float(line[(line.find("=") + 1) :])

        if len(time_cost) != 0:
            redis_connection.set("{}-progress_epoch".format(JOB_NAME), epoch)
            redis_connection.set("{}-progress_batch".format(JOB_NAME), batch)
            _set_dictionary(redis_connection, "{}-train-acc".format(JOB_NAME), train_acc)
            _set_dictionary(redis_connection, "{}-train-loss".format(JOB_NAME), train_loss)
            _set_dictionary(redis_connection, "{}-val-acc".format(JOB_NAME), val_acc)
            _set_dictionary(redis_connection, "{}-val-loss".format(JOB_NAME), val_loss)
            redis_connection.set("{}-time-cost".format(JOB_NAME), sum(time_cost.values()) / len(time_cost))

            logging.info(
                "Progress: Epoch: "
                + str(epoch)
                + ", Batch: "
                + str(batch)
                + ", Train-accuracy: "
                + str(train_acc)
                + ", Train-loss: "
                + str(train_loss)
                + ", Validation-accuracy: "
                + str(val_acc)
                + ", Validation-loss: "
                + str(val_loss)
                + ", Time-cost: "
                + str(time_cost)
            )


def _set_dictionary(redis_connection, key, value):
    """
    Helper function to save a dictionary in redis

    Args:
        redis_connection: connection to the redis database
        key (str): key for the value to be fetched from redis
        value (dict): value for given key, which must be json-serializable
    """
    redis_connection.set(key, json.dumps(value))


def _get_dictionary(redis_connection, key):
    """
    Helper function to get a dictionary from redis

    Args:
        redis_connection: connection to the redis database
        key (str): key for the value to be fetched from redis
    Returns:
        value for given key, which is of type dictionary‚
    """
    return json.loads(redis_connection.get(key))


def main():
    logfile = WORK_DIR + "training.log"
    logfile = "/data/training.log"
    if ROLE == "worker":
        update_progress(logfile)


if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("Description: monitor training progress in k8s cluster")
        print("Usage: python progress-monitor.py")
        sys.exit(1)
    main()
