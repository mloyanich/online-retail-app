import os
from setuptools import setup, find_packages

setup(
    include_package_data=True,
    install_requires=["uvicorn", "fastapi"],
    zip_safe=False,
    keywords="Retail Data application",
)
