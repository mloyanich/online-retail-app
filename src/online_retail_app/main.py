import os

import uvicorn
from api.product import router as product_router
from api.pipeline import router as pipeline_router
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


def get_application():
    application = FastAPI()
    application.include_router(product_router, prefix="/product")
    application.include_router(pipeline_router, prefix="/pipeline")

    origins = [
        "http://localhost:3000",
    ]

    application.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    return application


print("This is test message")

app = get_application()

if __name__ == "__main__":
    server_port = os.getenv("PORT") or 8000
    uvicorn.run("main:app", host="0.0.0.0", port=server_port, reload=True)
