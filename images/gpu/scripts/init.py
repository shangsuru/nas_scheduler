import logging
import os
import os.path
import sys
import threading
import time


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(module)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def main():
    logging.info("start init process ...")

    logging.info("start training thread ...")
    train = threading.Thread(target=(lambda: os.system("python /mxnet_mnist.py")), args=())
    train.setDaemon(True)
    train.start()


    monitor = threading.Thread(target=(lambda: os.system("python3 /monitor.py")), args=())
    monitor.setDaemon(True)
    monitor.start()



if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("Description: MXNet init script in k8s cluster")
        print("Usage: python init.py")
        sys.exit(1)
    main()
