# build and push
docker build --no-cache -t k8s-mxnet-gpu-full -f gpu.Dockerfile .
docker tag k8s-mxnet-gpu-full:latest localhost:5000/k8s-mxnet-gpu-full
docker push localhost:5000/k8s-mxnet-gpu-full
