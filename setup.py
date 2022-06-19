import setuptools

version_text = None
with open("finx_option_data/version.txt", "r", encoding="utf-8") as f:
    version_text = f.read()

with open("README.md", "r") as f:
    long_description = f.read()

deps = [
    # "boto3",
    # "fsspec",
    # "heroku3",
    "loguru",
    "pandas>1.2, <1.4",
    "psycopg2-binary",
    # "pyarrow",
    "python-dotenv",
    "pytz",
    "requests",
    # "s3fs",
    "sqlalchemy",
    # "tda-api",
]

test_deps = ["pytest"]

project_url = "https://github.com/westonplatter/finx-option-data"

setuptools.setup(
    name="finx_option_data",
    version=version_text,
    description="Fetch, store, and warehouse Option Data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="westonplatter",
    author_email="westonplatter+finx@gmail.com",
    license="BSD-3",
    url=project_url,
    python_requires=">=3.6",
    packages=["finx_option_data"],
    install_requires=deps,
    tests_require=test_deps,
    project_urls={
        "Issue Tracker": f"{project_url}/issues",
        "Source Code": f"{project_url}",
    },
)
