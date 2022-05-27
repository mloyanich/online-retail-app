from fastapi import APIRouter
import pandas as pd
from .product import ProductDataFrame

router = APIRouter()

products = ProductDataFrame()


@router.get("/pipeline")
def read_pipeline(skip: int = 0, limit: int = 100):
    return {"status": "success!!"}


@router.post("/pipeline")
def start_pipeline():
    df = products.load_df()
    transformed = products.transform()
    return {"status": "pipeline run success!!"}

@router.get("/product")
def read_products(skip: int = 0, limit: int = 100, search: str = ""):
    result = [row.asDict() for row in products.df.head(limit)]
    print('working!')
    if search:
        print(search)
        result = products.relative_products(search)

    return result
