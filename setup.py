from setuptools import setup, find_packages

setup(
    name="tundra",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "snowflake-connector-python",
        "pandas",
    ],
    author="Your Name",
    author_email="your.email@example.com",
    description="A Python library for Snowflake connectivity",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/thunderhugs/tundra",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)