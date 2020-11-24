import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="airtablecache",
    version="0.0.1",
    author="Raunak Khandelwal",
    author_email="raunakkhandelwal2000@yahoo.com",
    description="Utility Package for Caching Data From Airtable",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'xlrd',
        'numpy',
        'pandas',
        'gspread',
        'gspread-dataframe',
        'requests',
        'airtable-python-wrapper'
    ],
    package_data={'airtablecache': ['data/testData.csv']},
    python_requires='>=3.6',
    license='MIT',
    url="https://github.com/raunakk/airtablecache"
)
