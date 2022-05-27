from fastapi import APIRouter
from clippie.api.product import ProductDataFrame

router = APIRouter()

products = ProductDataFrame()


@router.post("/pipeline")
def start_pipeline_data_processing():
    df = products.load_df()
    transformed = products.transform()
    return {"status": "pipeline ran successfully!!"}

@router.get("/product")
def read_products(search: str = ""):
    result = [row.asDict() for row in products.df.head(100)]
    print('working!')
    if search:
        search = search.replace('"', '') 
        print(f'input product description is {search}')
        result = products.relative_products(search)

    return result
