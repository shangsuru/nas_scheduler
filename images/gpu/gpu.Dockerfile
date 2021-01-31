FROM localhost:5000/k8s-mxnet-gpu-base

# image-classification

COPY scripts/train_mnist.py /mxnet/example/image-classification/
COPY scripts/train_cifar10.py /mxnet/example/image-classification/
COPY scripts/train_imagenet.py /mxnet/example/image-classification/
COPY scripts/mxnet_mnist.py /mxnet/example/image-classification/
COPY scripts/fit.py /mxnet/example/image-classification/common/
COPY scripts/data.py /mxnet/example/image-classification/common/
RUN mkdir -p /mxnet/example/image-classification/data

# correcting paths
ENV PYTHONPATH $PYTHONPATH:/mxnet/example/image-classification/
# Install mxnet 1.7.0

#RUN pip install mxnet-cu100==1.7.0
RUN pip install redis watchdog


# Install dependencies for the mxnet_mnist.py testkript
#RUN pip install horovod[mxnet]
RUN pip uninstall -y typing
RUN pip install gluoncv

RUN pip install horovod[mxnet-cu100]

# Get resnet model
RUN mkdir -p /mxnet/example/image-classification/symbols
RUN wget https://raw.githubusercontent.com/yajiedesign/mxnet/v1.6.x_back/example/image-classification/symbols/resnet.py -O /mxnet/example/image-classification/symbols/resnet.py

# Get cifar training set
RUN wget http://data.mxnet.io/data/cifar10/cifar10_val.rec -O /mxnet/cifar10_val.rec
RUN wget http://data.mxnet.io/data/cifar10/cifar10_train.rec -O /mxnet/cifar10_train.rec

# scripts
COPY scripts/* /
CMD sleep 1000000000
