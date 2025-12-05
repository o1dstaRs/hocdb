from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="hocdb",
    version="0.1.1",
    author="Heroes of Crypto AI",
    author_email="",
    description="High-Performance Time Series Database Python Bindings",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hocai/hocdb",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    python_requires=">=3.8",
    install_requires=[],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-benchmark",
        ],
    },
    keywords="timeseries, database, high-performance, zig",
    project_urls={
        "Bug Reports": "https://github.com/hocai/hocdb/issues",
        "Source": "https://github.com/hocai/hocdb",
    },
)