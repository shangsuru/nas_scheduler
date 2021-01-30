# build and push
docker build --no-cache -t k8s-mxnet-gpu-base -f gpu_base.Dockerfile .
docker tag k8s-mxnet-gpu-base:latest localhost:5000/k8s-mxnet-gpu-base
docker push localhost:5000/k8s-mxnet-gpu-base
