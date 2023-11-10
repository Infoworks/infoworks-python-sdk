import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="infoworkssdk",
    version="4.0alpha6",
    author="Infoworks CE Team",
    author_email="abhishek.raviprasad@infoworks.io",
    description="A package to work with Infoworks via SDK. This library is compatible with Infoworks v5.5 onwards. Code can be found in https://github.com/Infoworks/InfoworksPythonSDK branch: release/sdk-3.0",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Infoworks/InfoworksPythonSDK",
    packages=setuptools.find_packages(),
    install_requires=[
        'requests', 'bson', 'urllib3', 'pandas', 'networkx', 'pyyaml'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
