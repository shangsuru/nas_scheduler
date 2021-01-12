import os
import logging
import time
import subprocess
import sys
import redis


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(module)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
ROLE = os.getenv("ROLE")
WORK_DIR = os.getenv("WORK_DIR")
JOB_NAME = os.getenv("JOB_NAME")

# read the log file and monitor the training progress
# give log file name
# give record file name
# run the function in a separate thread


def update_speed(logfile):
    filesize = 0
    line_number = 0

    redis_connection = redis.Redis()

    redis_connection.set("{}-avg_speed".format(JOB_NAME), 0)
    redis_connection.set("{}-stb_speed".format(JOB_NAME), 0)

    logging.info("starting speed monitor to track average training speed ...")

    speed_list = []
    while True:
        time.sleep(5)

        try:
            cursize = os.path.getsize(logfile)
        except OSError as e:
            logging.warning(e)
            continue
        if cursize == filesize:  # no changes in the log file
            continue
        else:
            filesize = cursize

        # Epoch[0] Time cost=50.885
        # Epoch[1] Batch [70]	Speed: 1.08 samples/sec	accuracy=0.000000
        with open(logfile, "r") as f:
            for i in range(line_number):
                f.readline()
            for line in f:
                line_number += 1

                start = line.find("Speed")
                end = line.find("samples")
                if start > -1 and end > -1 and end > start:
                    string = line[start:end].split(" ")[1]
                    try:
                        speed = float(string)
                        speed_list.append(speed)
                    except ValueError as e:
                        logging.warning(e)
                        break

        if len(speed_list) == 0:
            continue

        avg_speed = sum(speed_list) / len(speed_list)
        logging.info("Average Training Speed: " + str(avg_speed))

        stb_speed = 0
        if len(speed_list) <= 5:
            stb_speed = avg_speed
        else:
            pos = int(2 * len(speed_list) / 3)
            stb_speed = sum(speed_list[pos:]) / len(speed_list[pos:])  # only consider the later part

        logging.info("Stable Training Speed: " + str(stb_speed))

        redis_connection.set("{}-avg_speed".format(JOB_NAME), avg_speed)
        redis_connection.set("{}-stb_speed".format(JOB_NAME), stb_speed)


def main():
    logfile = "/data/training.log"
    # logfile = WORK_DIR + 'training.log'
    recordfile = WORK_DIR + "speed.txt"
    if ROLE == "worker":
        update_speed(logfile)


if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("Description: monitor training progress in k8s cluster")
        print("Usage: python update_progress.py")
        sys.exit(1)
    main()
