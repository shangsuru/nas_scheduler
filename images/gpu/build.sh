# build and push
docker build --no-cache -t k8s-mxnet-gpu-full-h -f gpu.Dockerfile .
docker tag k8s-mxnet-gpu-full-h:latest localhost:5000/k8s-mxnet-gpu-full-h
docker push localhost:5000/k8s-mxnet-gpu-full-h
