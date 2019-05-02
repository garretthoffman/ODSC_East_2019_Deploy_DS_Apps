# Social Sentiment With AWS Kinesis

In this section we will go ahead and start our Kinesis streaming application for aggregating our raw sentiment into a minutely sentiment score for each ticker. 

The application was actually already built when we launched our Cloudformation Stack. We deployed the following infrastrucure:

- [Kinesis Input Data Stream](https://github.com/GarrettHoffman/ODSC_East_2019_Deploy_DS_Apps/blob/master/tutorial-infra.yaml#L283-L287) that ingests our raw data
- [Lambda Function](https://github.com/GarrettHoffman/ODSC_East_2019_Deploy_DS_Apps/blob/master/tutorial-infra.yaml#L326-L386) that preprocesses the raw data with the following [code](https://github.com/GarrettHoffman/ODSC_East_2019_Deploy_DS_Apps/blob/master/02_streaming_analytics/01_kinesis/preprocess_sent_lambda.py) before feeding it into our application
- [Kinesis Data Analytics App](https://github.com/GarrettHoffman/ODSC_East_2019_Deploy_DS_Apps/blob/master/tutorial-infra.yaml#L409-L456) that runs the following [SQL Code](https://github.com/GarrettHoffman/ODSC_East_2019_Deploy_DS_Apps/blob/master/02_streaming_analytics/01_kinesis/agg_sent_minutely.sql)
- [Kinesis Delivery Firestream](https://github.com/GarrettHoffman/ODSC_East_2019_Deploy_DS_Apps/blob/master/tutorial-infra.yaml#L289-L301) that writes our application results to S3

Since all of the infra structure is deployed we just have to start our app and start sending our data. 

1. First go to the [Kinesis Dashboard](https://console.aws.amazon.com/kinesis/home?region=us-east-1#/dashboard) and open up our app. 

2. Click Go To SQL Editor and then click Yes, Start Application. You should see the SQL Editor, popuated with our aggregation code. The app will take quite a bit of time to load. Once it does, you should see an error that there are No rows in source stream. This is expected since we have't started pushing data to our stream yet.

3. Edit line 16 in `data_gen_lambda.py` with your Postgres host name:

```
HOST = os.environ.get('DB_HOST') or "<Your DB Host Name>"
```

4. Create our virtual environment, activate it and install package dependencies: 

```
conda env create -f environment.yml
conda activate kinesis
```

5. Run the data generation script:

```
python data_gen_lambda.py
```

6. Go back to Kinesis applicaiton and click retreive rows.

7. Click on Real-time analytics tab. Here we will see our data aggregating at the end of each minute. It might take 2 minutes to get a full batch of our data.

8. Go to the [S3 Bucket](https://github.com/GarrettHoffman/ODSC_East_2019_Deploy_DS_Apps/blob/master/tutorial-infra.yaml#L254-L258) that we defined for our streaming demo. Verify that our results are getting written as CSV files to S3.