from fastapi import APIRouter
import os
import json
import pandas as pd

router = APIRouter()


def read_folder_jsons(f_path):
    # print(type(f_path))
    result = []
    # json_folder_path =
    json_paths = [x
                  for x in os.listdir(f_path) if x.endswith(".json")]
    for json_file in json_paths:
        with open(f'{f_path}/{json_file}', 'r') as f:
            result.extend([json.loads(x) for x in f.readlines()])
    # print(jsons)
    return result


@ router.get("/")
def read_products(skip: int = 0, limit: int = 100):

    result = read_folder_jsons('/Users/masha/dev/git/py/skupos/products_json')
    return result[skip:skip+limit]
