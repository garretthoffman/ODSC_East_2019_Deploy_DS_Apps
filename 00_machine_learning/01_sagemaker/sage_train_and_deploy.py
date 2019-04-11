import sagemaker
from sagemaker.tensorflow import TensorFlow
import json

# define S3 locations for data inputs
s3_prefix = 'tf-chart-cv'

train_data_s3_uri = f's3://{s3_prefix}/data/train'
test_data_s3_uri = f's3://{s3_prefix}/data/test'

inputs = {'train': train_data_s3_uri, 'test': test_data_s3_uri}

# other inputs
model_dir = '/opt/ml/model'
train_instance_type = 'ml.p3.2xlarge'

# load hyperparameters from hyperparameter config file
with open("hyperparameters.json") as f:
    hyperparameters = json.load(f)

# create sagemaker TF Estimator for hosted training
estimator = TensorFlow(entry_point='train.py',
                       model_dir=model_dir,
                       train_instance_type=train_instance_type,
                       train_instance_count=1,
                       hyperparameters=hyperparameters,
                       role='AmazonSageMaker-ExecutionRole-20181205T225138',
                       base_job_name='test-tf-chart-cv',
                       framework_version='1.12.0',
                       py_version='py3',
                       script_mode=True)

# call to start hosted training
estimator.fit(inputs)

# deploy our estimator
estimator.deploy(initial_instance_count=1, instance_type='ml.m5.large')