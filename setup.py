import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="infoworkssdk",
    version="1.1.0",
    author="Abhishek",
    author_email="abhishek.raviprasad@infoworks.io",
    description="A package to work with Infoworks via SDK",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Infoworks/InfoworksPythonSDK",
    packages=setuptools.find_packages(),
    install_requires=[
        'requests', 'bson', 'pycryptodomex>=3.13.0', 'cryptography==37.0.2', 'urllib3', 'pandas', 'networkx',
        'pyyaml', 'google-cloud-kms',
        'google-cloud-storage', 'boto3', 'azure-identity', 'azure-keyvault-secrets'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
