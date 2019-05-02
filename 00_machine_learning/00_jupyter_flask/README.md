# Chart Classification Model with Flask, Docker and Kubernetes

In this section we deploy our trained model as a REST API on our EKS (Kubernetes) Cluster. We will build our API with flask.

First we train our model using a [Jupyter Notebook](https://github.com/GarrettHoffman/ODSC_East_2019_Deploy_DS_Apps/blob/master/00_machine_learning/00_jupyter_flask/model/Chart%20vs.%20Meme%20Classification%20Model.ipynb) and save our model out to our [api directory](https://github.com/GarrettHoffman/ODSC_East_2019_Deploy_DS_Apps/tree/master/00_machine_learning/00_jupyter_flask/api). This process involved putting together our training data set locally, tuning parameters inside the jupyter notebook and tinkering and fiddling until we are happy with our model performance. This is done for us already.

Now that we have our trained model we will deploy our API.

1. Navigate to the `/api` directroy:
```
cd /api
```

2. Our API is defined in `app.py`. This has two endpoints. 
    - `/chart_classifier/predict` takes an image, transforms the image to our model input, and returns a prediction. 
    - `/health` is a standard endpoint that most APIs have to return the health status of the app. This endpoint is periodically hit by the load balancer to make sure that the app is healthy and it is also hit by Kubernetes before it is will make the service available.

3. First we build a docker image of our API:

```
docker build -t ml-api -f Dockerfile .
```

4. Now we want to tag and push this to our [Remote ECR Repo](https://github.com/GarrettHoffman/ODSC_East_2019_Deploy_DS_Apps/blob/master/tutorial-infra.yaml#L33-L53) that we spun up earlier. Replace `<Your AWS Account ID>` with your AWS account ID.
```
docker tag ml-api:latest <Your AWS Account ID>.dkr.ecr.us-east-1.amazonaws.com/ml-api:latest
```
```
docker push <Your AWS Account ID>.dkr.ecr.us-east-1.amazonaws.com/ml-api:latest
```

5. Now that our image is in our remote repository we deploy our Kubernetes Deployment and Service. This is defined in `ml-api.yaml` and deploying this will do two things:
    - It will pull the image from our remote repository and deploy our API server as a Pod on our EKS Cluster
    - It will put a public endpoint infront of it that will allow us to direct traffic to our API.

```
kubectl apply -f ml-api.yaml -n datascience
```

6. We can monitor the deployments status by running the following command in a new tab:
```
kubectl rollout status deployment ml-api -n datascience
```

7. Once our deployment is done we need to get our endpoint:
```
kubectl describe svc ml-api-service -n datascience
```

8. Copy our API endpoint into line 6 of `request/py`
```
API_ENDPOINT = "<Your ML API Endpoint>/chart_classifier/predict"
```

9. Send a request to the endpoint:
```
python request.py -i ../data/awesome_chart.png
```

