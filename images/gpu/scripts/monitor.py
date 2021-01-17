import json
import logging
import os
import redis
import sys
import time

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


class Monitor:
    def __init__(self) -> None:
        self.observer = Observer()
        self.running = False

    def run(self):
        try:
            logging.info("Monitor started.")
            self.observer.schedule(TrainingWatcher(), "/data")
            self.observer.start()
            self.running = True
            while self.running:
                time.sleep(1)
        except:
            self.observer.stop()
        self.observer.join()

    def stop(self):
        self.running = False


class TrainingWatcher(PatternMatchingEventHandler):
    def __init__(self):
        super().__init__(patterns=["*training.log"])
        self.batch = 0
        self.epoch = 0
        self.filesize = 0
        self.keys = [
            "{}-progress".format(JOB_NAME),
            "{}-train_acc".format(JOB_NAME),
            "{}-train-loss".format(JOB_NAME),
            "{}-val-acc".format(JOB_NAME),
            "{}-val-loss".format(JOB_NAME),
            "{}-time-cost".format(JOB_NAME),
        ]
        self.logfile = "/data/training.log"
        self.last_change = time.time()
        self.line_num = 0
        self.redis_connection = redis.Redis("130.83.143.241")
        self.speed_list = []
        self.time_cost = {}
        self.train_acc = {}
        self.train_loss = {}
        self.val_acc = {}
        self.val_loss = {}

        # Set default values
        self.redis_connection.set("{}-stb_speed".format(JOB_NAME), 0)
        self.redis_connection.set("{}-avg_speed".format(JOB_NAME), 0)
        for key in self.keys:
            self.redis_connection.set(key, 0)

    def on_modified(self, event):
        """Event handling when the training file is modified

        Args:
            event(watchdog.events.event): FileModifiedEvent for the training log file(used by watchdog API)
        """

        # Modifying a file created two events instantly, we need to ignore the duplicate
        tic = time.time()
        if tic - self.last_change < 0.05:
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

                epoch = int(line[line.find("[", epoch_index) + 1 : line.find("]", epoch_index)])

                # batch number
                # TODO batches are now grouped
                batch_index = line.find("Batch")
                if batch_index > -1:
                    self.batch = int(line[line.find("[", batch_index) + 1 : line.find("-", batch_index)])
                else:
                    self.batch = -1  # the end of this epoch

                # Retrieve mertrics
                train_acc_index = line.find("Train-accuracy")
                if train_acc_index > -1:
                    self.train_acc[epoch] = float(line[(line.find("=") + 1) :])

                train_loss_index = line.find("Train-cross-entropy")
                if train_loss_index > -1:
                    self.train_loss[epoch] = float(line[(line.find("=") + 1) :])

                val_acc_index = line.find("Validation-accuracy")
                if val_acc_index > -1:
                    self.val_acc[epoch] = float(line[(line.find("=") + 1) :])

                val_loss_index = line.find("Validation-cross-entropy")
                if val_loss_index > -1:
                    self.val_loss[epoch] = float(line[(line.find("=") + 1) :])

                time_cost_index = line.find("Time cost")
                if time_cost_index > -1:
                    self.time_cost[epoch] = float(line[(line.find("=") + 1) :])

                start = line.find("Speed")
                end = line.find("samples")
                if start > -1 and end > -1 and end > start:
                    string = line[start:end].split(" ")[1]
                    try:
                        speed = float(string)
                        self.speed_list.append(speed)
                    except ValueError as e:
                        logging.warning(e)
                        break

        if len(self.time_cost) != 0:
            self.redis_connection.set("{}-progress_epoch".format(JOB_NAME), self.epoch)
            self.redis_connection.set("{}-progress_batch".format(JOB_NAME), self.batch)
            self._set_dictionary("{}-train-acc".format(JOB_NAME), self.train_acc)
            self._set_dictionary("{}-train-loss".format(JOB_NAME), self.train_loss)
            self._set_dictionary("{}-val-acc".format(JOB_NAME), self.val_acc)
            self._set_dictionary("{}-val-loss".format(JOB_NAME), self.val_loss)
            self.redis_connection.set(
                "{}-time-cost".format(JOB_NAME), sum(self.time_cost.values()) / len(self.time_cost)
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

            self.redis_connection.set("{}-avg_speed".format(JOB_NAME), avg_speed)
            self.redis_connection.set("{}-stb_speed".format(JOB_NAME), stb_speed)

    def _set_dictionary(self, key, value):
        """
        Helper function to save a dictionary in redis

        Args:
            redis_connection: connection to the redis database
            key (str): key for the value to be fetched from redis
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
