# online-retail-app

This is a FastAPI application that accepts a product description as input and returns the top 10 most similar products that are in the transaction data.

In order to install the app run

```bash
pip install -e .
```

Available endpoints:

- `/docs` - GET - API documentation
- `/product` - GET - displays list of products
- `/product?search=coala` - GET - find relevant products to the provided desciption
- `/pipeline` - POST

Run the application with the following command

```bash
online-retail-app
```

or

```bash
python3 src/online_retail_app/main.py
```

Upon start the application loads sample dataset that is located in `data` folder

# TODO

- [ ] package Java jar file in order to open excel with pyspark
- [ ] enable tempfile
- [ ] run the application
- [ ] GET pipeline to see the number of pipeline that has been executed
- [ ] add history of pipeline execution to GET pipeline
- [ ] what will happen if pipelines run in parallel?
- [ ] description in a body of product GET
- [x] precalculate number of words in entire dataset
- [x] what if I pass a new word to product GET?
- [x] product should be more than 0.0
- [x] BUG - product search stopped working
