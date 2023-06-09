import json
import logging
import os
import re
import redis
import sys
import time

from typing import Any, Match, Optional
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(module)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

ROLE = os.getenv("ROLE")
WORK_DIR = os.getenv("WORK_DIR")
JOB_NAME = os.getenv("JOB_NAME")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REPLICA_ID = os.getenv("REPLICA_ID")


class Parser:
    """
    Interface class for Parser instances
    """

    def epoch(self, input: str) -> Optional[Match[Any]]:
        """
        Parses input for epoch
        """
        return self.epoch_pattern.search(input)

    def batch(self, input: str) -> Optional[Match[Any]]:
        """
        Parses input for batch
        """
        return self.batch_pattern.search(input)

    def speed(self, input: str) -> Optional[Match[Any]]:
        """
        Parses input for speed
        """
        return self.speed_pattern.search(input)

    def train_acc(self, input: str) -> Optional[Match[Any]]:
        """
        Parses input for training accuracy
        """
        return self.train_acc_pattern.search(input)

    def train_ce(self, input: str) -> Optional[Match[Any]]:
        """
        Parses input for training cross-entropy
        """
        return self.train_ce_pattern.search(input)

    def val_acc(self, input: str) -> Optional[Match[Any]]:
        """
        Parses input for validation accuracy
        """
        return self.val_acc_pattern.search(input)

    def val_ce(self, input: str) -> Optional[Match[Any]]:
        """
        Parses input for validation cross-entropy
        """
        return self.val_ce_pattern.search(input)

    def time_cost(self, input: str) -> Optional[Match[Any]]:
        """
        Parses input for time cost
        """
        return self.time_cost_pattern.search(input)


class MXNetParser(Parser):
    epoch_pattern = re.compile(r"Epoch\s*\[?\s*(?P<epoch>\d+)")
    batch_pattern = re.compile(r"Batch\s*\[?\s*(?P<batch>\d+)")
    speed_pattern = re.compile(r"Speed(:|=)\s*(?P<speed>\d+.\d+)")
    train_acc_pattern = re.compile(r"Train(ing)?\s*(:|-)\s*accuracy\s*=\s*(?P<train_acc>\d+.\d+)")
    train_ce_pattern = re.compile(r"Train(ing)?\s*(:|-)\s*cross-entropy\s*=\s*(?P<train_ce>\d+.\d+)")
    val_acc_pattern = re.compile(r"Validation\s*(:|-)\s*accuracy\s*=\s*(?P<val_acc>\d+.\d+)")
    val_ce_pattern = re.compile(r"Validation\s*(:|-)\s*cross-entropy\s*=\s*(?P<val_ce>\d+.\d+)")
    time_cost_pattern = re.compile(r"Time cost=(?P<time_cost>\d+.\d+)")


class PyTorchParser(Parser):
    # TODO
    pass


class TFParser(Parser):
    # TODO
    pass


class Monitor:
    """
    Class that actively monitors a training jobs via its log file

    Fields:
        observer (watchdog.observers.Observer): Observer object watching for file changes in given directory
        running (bool): True if Monitor is active else False
    """

    def __init__(self) -> None:
        self.observer = Observer()
        self.running = False

    def run(self):
        """
        Activate Monitor
        """
        logging.info("Monitor started.")
        logging.info("TRAINING_LOG_DIR={}".format(os.getenv("TRAINING_LOG_DIR")))
        logging.info("TRAINING_LOG_FILE={}".format(os.getenv("TRAINING_LOG_FILE")))
        self.observer.schedule(TrainingWatcher(), os.getenv("TRAINING_LOG_DIR"))
        self.observer.start()
        self.running = True

    def stop(self):
        """
        Deactivate Monitor
        """
        if self.running:
            self.running = False
            self.observer.stop()
            self.observer.join()


class TrainingWatcher(PatternMatchingEventHandler):
    """
    Parses log file on file changes and extracts metrics

    Fields:
        batch (int): Batch number
        epoch (int): Epoch number
        filesize (int): Size of file prior to change
        keys (list[str]): Redis key values of metrics
        logfile (str): Path to log file
        last_change (timestamp): Timestamp of last change to file, this is used to prevent
            the Watcher triggering two times on file change (Two changes are detecte even if
            file was only changed once)
        line_num (int): Line number of the last line that has been read so far
        redis_connection (redis.Redis): Redis API
        speed_list (list[float]): List of speeds recorded so far
        time_cost (dict): Time cost for specified epoch
        train_acc (dict): Training accuracy for specified epoch
        train_loss (dict): Training loss for specified epoch
        val_acc (dict): Validation accuracy for specified epoch
        val_loss (dict): Validation loss for specified epoch

        _*****_pattern (re): regex patterns to extract metrics from log file
    """

    def __init__(self):
        super().__init__(patterns=["*" + str(os.getenv("TRAINING_LOG_FILE"))])
        self.batch = 0
        self.epoch = 0
        self.filesize = 0
        self.keys = [
            f"{JOB_NAME}-{REPLICA_ID}-progress",
            f"{JOB_NAME}-{REPLICA_ID}-train_acc",
            f"{JOB_NAME}-{REPLICA_ID}-train-loss",
            f"{JOB_NAME}-{REPLICA_ID}-val-acc",
            f"{JOB_NAME}-{REPLICA_ID}-val-loss",
            f"{JOB_NAME}-{REPLICA_ID}-time-cost",
        ]
        self.logfile = str(os.getenv("TRAINING_LOG_DIR")) + str(os.getenv("TRAINING_LOG_FILE"))
        self.last_change = 0
        self.line_num = 0
        self.redis_connection = redis.Redis(str(REDIS_HOST), int(REDIS_PORT))
        self.speed_list = []
        self.time_cost = {}
        self.train_acc = {}
        self.train_loss = {}
        self.val_acc = {}
        self.val_loss = {}

        framework = os.getenv("FRAMEWORK")
        self.parser: Parser = None
        if framework == "mxnet":
            self.parser = MXNetParser()
        elif framework == "tf":
            self.parser = TFParser()
        elif framework == "pytorch":
            self.parser = PyTorchParser()
        else:
            raise Exception(f"Framework is not supported: {framework}")

        # mark keys as invalid by setting them to -1
        self.redis_connection.set(f"{JOB_NAME}-{REPLICA_ID}-stb_speed", -1)
        self.redis_connection.set(f"{JOB_NAME}-{REPLICA_ID}-avg_speed", -1)
        for key in self.keys:
            self.redis_connection.set(key, -1)

    def on_modified(self, event):
        """
        Callback when logfile has changed. Tries to extract metrics from log file.

        Args:
            event(watchdog.events.event): FileModifiedEvent for the training log file(used by watchdog API)
        """

        # Modifying a file created two events instantly, we need to ignore the duplicate
        tic = time.time()
        if tic - self.last_change < 0.005:
            return
        self.last_change = tic

        try:
            current_size = os.path.getsize(self.logfile)
        except OSError as e:
            logging.warning(e)
            return

        # File has not changed which should not happen
        if current_size == self.filesize:
            logging.warn("Watchdog triggered even though training log file has not changed in length")
            return

        self.filesize = current_size

        with open(self.logfile, "r") as f:
            for _ in range(self.line_num):
                f.readline()
            for line in f:
                self.line_num += 1

                line = line.replace("\n", "")

                epoch_index = line.find("Epoch")

                # line should begin with epoch number
                if epoch_index < 0:
                    continue
                self.parse_epoch(line)
                # batch number
                self.parse_batch(line)

                # Retrieve mertrics
                self.parse_train_acc(line)
                self.parse_train_ce(line)
                self.parse_val_acc(line)
                self.parse_val_ce(line)
                self.parse_time_cost(line)
                self.parse_speed(line)

        if len(self.time_cost) != 0:
            print(self.epoch)
            self.redis_connection.set(f"{JOB_NAME}-{REPLICA_ID}-progress_epoch", self.epoch)
            self.redis_connection.set(f"{JOB_NAME}-{REPLICA_ID}-progress_batch", self.batch)
            self._set_dictionary(f"{JOB_NAME}-{REPLICA_ID}-train-acc", self.train_acc)
            self._set_dictionary(f"{JOB_NAME}-{REPLICA_ID}-train-loss", self.train_loss)
            self._set_dictionary(f"{JOB_NAME}-{REPLICA_ID}-val-acc", self.val_acc)
            self._set_dictionary(f"{JOB_NAME}-{REPLICA_ID}-val-loss", self.val_loss)
            self.redis_connection.set(
                f"{JOB_NAME}-{REPLICA_ID}-time-cost", sum(self.time_cost.values()) / len(self.time_cost)
            )

            logging.info(
                "Progress: Epoch: "
                + str(self.epoch)
                + ", Batch: "
                + str(self.batch)
                + ", Train-accuracy: "
                + str(self.train_acc)
                + ", Train-loss: "
                + str(self.train_loss)
                + ", Validation-accuracy: "
                + str(self.val_acc)
                + ", Validation-loss: "
                + str(self.val_loss)
                + ", Time-cost: "
                + str(self.time_cost)
            )

        if len(self.speed_list) > 0:
            avg_speed = sum(self.speed_list) / len(self.speed_list)
            logging.info("Average Training Speed: " + str(avg_speed))

            stb_speed = 0
            if len(self.speed_list) <= 5:
                stb_speed = avg_speed
            else:
                pos = int(2 * len(self.speed_list) / 3)
                stb_speed = sum(self.speed_list[pos:]) / len(self.speed_list[pos:])  # only consider the later part

            logging.info("Stable Training Speed: " + str(stb_speed))

            self.redis_connection.set(f"{JOB_NAME}-{REPLICA_ID}-avg_speed", avg_speed)
            self.redis_connection.set(f"{JOB_NAME}-{REPLICA_ID}-stb_speed", stb_speed)

    def parse_epoch(self, string):
        """
        Looks for epoch in given string, sets epoch if found

        Args:
            string (str): String in which the search should commence
        """
        res = self.parser.epoch(string)
        if res:
            self.epoch = int(res.group("epoch"))

    def parse_batch(self, string):
        """
        Looks for batch in given string, sets batch if found

        Args:
            string (str): String in which the search should commence
        """
        # TODO batches are now grouped
        res = self.parser.batch(string)
        if res:
            self.batch = int(res.group("batch"))
        else:
            self.batch = -1  # the end of this epoch

    def parse_train_acc(self, string):
        """
        Looks for training accuracy in given string, sets training accuracy if found

        Args:
            string (str): String in which the search should commence
        """
        res = self.parser.train_acc(string)
        if res:
            self.train_acc[self.epoch] = float(res.group("train_acc"))

    def parse_train_ce(self, string):
        """
        Looks for training loss in given string, sets training loss if found

        Args:
            string (str): String in which the search should commence
        """
        res = self.parser.train_ce(string)
        if res:
            self.train_loss[self.epoch] = float(res.group("train_ce"))

    def parse_val_acc(self, string):
        """
        Looks for validation accuracy in given string, sets validation accuracy if found

        Args:
            string (str): String in which the search should commence
        """
        res = self.parser.val_acc(string)
        if res:
            self.val_acc[self.epoch] = float(res.group("val_acc"))

    def parse_val_ce(self, string):
        """
        Looks for validation loss in given string, sets validation loss if found

        Args:
            string (str): String in which the search should commence
        """
        res = self.parser.val_ce(string)
        if res:
            self.val_loss[self.epoch] = float(res.group("val_ce"))

    def parse_time_cost(self, string):
        """
        Looks for time cost in given string, sets time cost if found

        Args:
            string (str): String in which the search should commence
        """
        res = self.parser.time_cost(string)
        if res:
            self.time_cost[self.epoch] = float(res.group("time_cost"))

    def parse_speed(self, string):
        """
        Looks for speed in given string, sets speed if found

        Args:
            string (str): String in which the search should commence
        """
        res = self.parser.speed(string)
        if res:
            self.speed_list.append(float(res.group("speed")))

    def _set_dictionary(self, key, value):
        """
        Helper function to save a dictionary in redis

        Args:
            redis_connection: connection to the redis database
            key (str): key for the value to be set in redis
            value (dict): value for given key, which must be json-serializable
        """
        self.redis_connection.set(key, json.dumps(value))

    def _get_dictionary(self, key):
        """
        Helper function to get a dictionary from redis

        Args:
            redis_connection: connection to the redis database
            key (str): key for the value to be fetched from redis
        Returns:
            value for given key, which is of type dictionary‚
        """
        return json.loads(self.redis_connection.get(key))


if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("Description: monitor training progress in k8s cluster")
        print("Usage: python progress-monitor.py")
        sys.exit(1)
    if ROLE == "worker":
        Monitor().run()
        while True:
            time.sleep(1)
