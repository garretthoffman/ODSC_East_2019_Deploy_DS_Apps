# Graph Recommendations

In this section we will demonstrate how to perform a batch processing task to pull out user and room recommendations from a users social graph. 

First we need to seed our database with some mock data.

1. Edit line 9 in `data_gen_lambda.py` with your Postgres host name:

```
HOST = os.environ.get('DB_HOST') or "<Your DB Host Name>"
```

2. Create our virtual environment, activate it and install package dependencies: 

```
conda env create -f environment.yml
conda activate batch-data-gen
```

3. Run the data generation script:

```
python data_gen_lambda.py
```