import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="daily_query",
    version="0.0.1",
    author="EC",
    author_email="ceduth@techoutlooks.com",
    description="Database helper that assumes that daily data is "
                "stored under a distinct collection named after that date.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pypa/sampleproject",
    project_urls={
        "Bug Tracker": "https://bitbucket.com/techoutlooks/ARISE/dbutils/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
    install_requires=[
        # "Do not install the “bson” package from pypi. PyMongo comes with its own ..."
        # https://pypi.org/project/pymongo/
        # 'bson',
        'pymongo',
        'ordered_set',
    ],
)
