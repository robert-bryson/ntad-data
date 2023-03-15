from setuptools import setup

setup(
    name="ntad-data",
    version="0.0.1",
    author="Robert Bryson",
    description="Lambdas for updating an NTAD AGOL map",
    url="https://github.com/robert-bryson/ntad-data",
    python_requires="3.9",
    install_requires=["python-dotenv", "cryptography", "arcgis", "boto3"],
)
