import config
import json
import os
import random
import redis
import time


os.environ["TRAINING_LOG_DIR"] = "./"
os.environ["TRAINING_LOG_FILE"] = "training.log"
os.environ["ROLE"] = "WORKER"
os.environ["JOB_NAME"] = "TESTMONITOR"
os.environ["REDIS_HOST"] = config.REDIS_HOST_DAEMON_CLIENT
os.environ["REDIS_PORT"] = str(config.REDIS_PORT_DAEMON_CLIENT)
from images.gpu.scripts.monitor import Monitor, TrainingWatcher


def jsonKeys2int(x):
    """
    Convert keys of a dict retrieved via json methods to integers

    Args:
        x(dict): Dictionary retrieved via json methods
    """
    if isinstance(x, dict):
        return {int(k): v for k, v in x.items()}
    return x


class TestMonitor:
    """
    Tests for monitor observing jobs status in pods

    Fields:
    monitor: Monitor object(refer to monitor.py)
    redis_connection: Redis API object
    tw: TrainingWatcher object(refer to monitor.py)
    """

    monitor = Monitor()
    redis_connection = redis.Redis(config.REDIS_HOST_DAEMON_CLIENT, config.REDIS_PORT_DAEMON_CLIENT)
    tw = TrainingWatcher()

    def test_watch_dog_trigger1(self):
        """
        Test if the watch dog triggers correctly and set the correct metric values in Redis
        """

        # create temporary log file
        logfile = open("./training.log", "w+")
        self.monitor.run()
        time.sleep(0.5)

        # replicate metrics stored in Monitor/TrainingWatcher
        speed_list = []
        accuracy = 0
        cross_entropy = 3
        train_accuracy_lst = {}
        train_cross_entropy_lst = {}
        validation_accuracy_lst = {}
        validation_cross_entropy_lst = {}
        time_cost_lst = {}
        for epoch in range(3):
            for batch in range(5):
                speed_list.append(random.uniform(90, 120))
                accuracy = random.uniform(accuracy, 1)
                cross_entropy = random.uniform(0, cross_entropy)
                log = (
                    f"Epoch[{epoch}] Batch [{batch}-{batch}] Speed: {speed_list[-1]} samples/s"
                    f"accuracy={accuracy} cross-entropy={cross_entropy}\n"
                )
                logfile.write(log)
                logfile.flush()
                time.sleep(1)

                # check metrics calculation for speed
                avg_speed = sum(speed_list) / len(speed_list)
                if len(speed_list) <= 5:
                    stb_speed = avg_speed
                else:
                    pos = int(2 * len(speed_list) / 3)
                    stb_speed = sum(speed_list[pos:]) / len(speed_list[pos:])  # only consider the later part

                stb_speed_mon = float(self.redis_connection.get("TESTMONITOR-stb_speed"))
                avg_speed_mon = float(self.redis_connection.get("TESTMONITOR-avg_speed"))

                assert stb_speed_mon == stb_speed
                assert avg_speed_mon == avg_speed

            # check if metrics are correctly set after epoch
            train_accuracy_lst[epoch] = random.uniform(0, 1)
            train_cross_entropy_lst[epoch] = random.uniform(0, 1)
            validation_accuracy_lst[epoch] = random.uniform(0, 1)
            validation_cross_entropy_lst[epoch] = random.uniform(0, 1)
            time_cost_lst[epoch] = random.uniform(30, 50)
            log = (
                f"Epoch [{epoch}] Train-accuracy={train_accuracy_lst[epoch]}\n"
                f"Epoch [{epoch}] Train-cross-entropy={train_cross_entropy_lst[epoch]}\n"
                f"Epoch [{epoch}] Validation-accuracy={validation_accuracy_lst[epoch]}\n"
                f"Epoch [{epoch}] Validation-cross-entropy={validation_cross_entropy_lst[epoch]}\n"
                f"Epoch [{epoch}] Time cost={time_cost_lst[epoch]}\n"
            )
            logfile.write(log)
            logfile.flush()
            time.sleep(1)

            assert epoch == int(self.redis_connection.get("TESTMONITOR-progress_epoch"))
            assert -1 == int(self.redis_connection.get("TESTMONITOR-progress_batch"))
            assert train_accuracy_lst == json.loads(
                self.redis_connection.get("TESTMONITOR-train-acc"), object_hook=jsonKeys2int
            )
            assert train_cross_entropy_lst == json.loads(
                self.redis_connection.get("TESTMONITOR-train-loss"), object_hook=jsonKeys2int
            )
            assert validation_accuracy_lst == json.loads(
                self.redis_connection.get("TESTMONITOR-val-acc"), object_hook=jsonKeys2int
            )
            assert validation_cross_entropy_lst == json.loads(
                self.redis_connection.get("TESTMONITOR-val-loss"), object_hook=jsonKeys2int
            )
            assert sum(time_cost_lst.values()) / len(time_cost_lst) == float(
                self.redis_connection.get("TESTMONITOR-time-cost")
            )
        self.monitor.stop()
        os.remove("./training.log")

    def test_parse_epoch(self):
        """
        Test parsing for epoch
        """
        self.tw.parse_epoch(
            "Epoch[998] Batch [9-9] Speed: 96.14435502069577 samples/saccuracy=0.9996545674072476 "
            "cross-entropy=0.000504209381099434"
        )
        assert self.tw.epoch == 998
        self.tw.parse_epoch(
            "[Epoch 999 Batch 9] Speed: 96.14435502069577 samples/saccuracy=0.9996545674072476 "
            "cross-entropy=0.000504209381099434"
        )
        assert self.tw.epoch == 999

        self.tw.parse_epoch("junk")
        assert self.tw.epoch == 999

    def test_parse_batch(self):
        """
        Test parsing for batch
        """
        self.tw.parse_batch("Epoch[1] Batch [998-1000] Speed: 96.14435502069577 samples/s accuracy=0.9996545674072476 ")
        assert self.tw.batch == 998
        self.tw.parse_batch("[Epoch 4 Batch 999] Training: accuracy=0.1312312")
        assert self.tw.batch == 999
        self.tw.parse_batch("Epoch[2]")
        assert self.tw.batch == -1
        self.tw.parse_batch("junk")
        assert self.tw.batch == -1

    def test_parse_train_acc(self):
        """
        Test parsing for training accuracy
        """
        self.tw.epoch = 1
        self.tw.parse_train_acc("[Epoch 1] Training: accuracy=0.1312312")
        assert self.tw.train_acc[1] == 0.1312312
        self.tw.epoch = 2
        self.tw.parse_train_acc("Epoch[2] Training-accuracy=0.25")
        assert self.tw.train_acc[2] == 0.25
        self.tw.parse_train_acc("junk")
        assert self.tw.train_acc[2] == 0.25

    def test_parse_train_ce(self):
        """
        Test parsing for training loss
        """
        self.tw.epoch = 1
        self.tw.parse_train_ce("[Epoch 1] Training: cross-entropy=1.67665")
        assert self.tw.train_loss[1] == 1.67665
        self.tw.epoch = 2
        self.tw.parse_train_ce("Epoch[2] Training-cross-entropy=2.955")
        assert self.tw.train_loss[2] == 2.955
        self.tw.parse_train_ce("junk")
        assert self.tw.train_loss[2] == 2.955

    def test_parse_val_acc(self):
        """
        Test parsing for validation accuracy
        """
        self.tw.epoch = 1
        self.tw.parse_val_acc("[Epoch 1] Validation: accuracy=0.4")
        assert self.tw.val_acc[1] == 0.4
        self.tw.epoch = 2
        self.tw.parse_val_acc("Epoch[2] Validation-accuracy=0.325")
        assert self.tw.val_acc[2] == 0.325
        self.tw.parse_val_acc("junk")
        assert self.tw.val_acc[2] == 0.325

    def test_parse_val_ce(self):
        """
        Test parsing for validation loss
        """
        self.tw.epoch = 1
        self.tw.parse_val_ce("[Epoch 1] Validation: cross-entropy=1.4")
        assert self.tw.val_loss[1] == 1.4
        self.tw.epoch = 2
        self.tw.parse_val_ce("Epoch[2] Validation-cross-entropy=4.1")
        assert self.tw.val_loss[2] == 4.1
        self.tw.parse_val_ce("Epoch[2] Validation blah")
        assert self.tw.val_loss[2] == 4.1

    def test_parse_time_cost(self):
        """
        Test parsing for time cost
        """
        self.tw.epoch = 1
        self.tw.parse_time_cost("Epoch[1] Speed=123.123 samples/s Time cost=6.02")
        assert self.tw.time_cost[1] == 6.02
        self.tw.epoch = 2
        self.tw.parse_time_cost("Epoch[2] Time cost=3.5")
        assert self.tw.time_cost[2] == 3.5
        self.tw.parse_time_cost("junk")
        assert self.tw.time_cost[2] == 3.5

    def test_parse_speed(self):
        """
        Test parsing for speed
        """
        self.tw.parse_speed("Epoch[1] Batch[3] Speed: 118.99 samples/s")
        assert self.tw.speed_list[-1] == 118.99
        self.tw.parse_speed("Epoch[1] Batch[3] Speed: 5.1samples/s")
        assert self.tw.speed_list[-1] == 5.1
        l = len(self.tw.speed_list)
        self.tw.parse_speed("junk")
        assert self.tw.speed_list[-1] == 5.1
        assert l == len(self.tw.speed_list)
