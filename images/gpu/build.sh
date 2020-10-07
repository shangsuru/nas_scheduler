# build and push
docker build --no-cache -t k8s-mxnet-gpu -f gpu.Dockerfile .
docker tag k8s-mxnet-gpu:latest localhost:5000/k8s-mxnet-gpu
docker push localhost:5000/k8s-mxnet-gpu
