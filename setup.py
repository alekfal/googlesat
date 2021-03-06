import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open('requirements.txt') as f:
    required = f.read().splitlines()

setuptools.setup(
    name = "googlesat",
    version = "0.0.1",
    author = "Falagas Alekos",
    author_email = "alek.falagas@gmail.com",
    description = "Download Sentinel 2 data from GCP",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    url="nope.gg",
    packages = setuptools.find_packages(),
    include_package_data=True,
    license="MIT",
    classifiers = [
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires = '>=3.6',
    dependenct_links=[""],
    install_requires=[required]
)