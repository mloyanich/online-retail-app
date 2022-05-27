# online-retail-app

This is a FastAPI application that accepts a product description as input and returns the top 10 most similar products that are in the transaction data.
The dataset used in this application is located at the [following URL](https://archive.ics.uci.edu/ml/datasets/online+retail)

## Install

```bash
pip install clippie
```

Install the application from the local directory

```bash
pip install -e .
```

## Run

Run the application with the following command

```bash
clippie
```

or

```bash
python3 src/clippie/main.py
```

Upon start the application loads sample dataset that is located in the `data` folder

## API endpoints

Application listens on http://localhost:8000

Available endpoints:

- `/docs` - GET - API documentation
- `/product` - GET - displays list of products
- `/product?search=coala` - GET - find relevant products to the provided description
- `/pipeline` - POST

## TODO

- [ ] package Java jar file in order to open excel with pyspark
- [ ] enable tempfile
- [ ] add history of pipeline execution to GET pipeline
- [ ] GET pipeline to see the number of pipeline that has been executed
- [ ] the collision will happen if pipelines run in parallel?
- [ ] Github action
- [ ] Stop Spark on program termination
- [ ] BUG - spark is loaded twice!
- [ ] add debug mode with reload=True
