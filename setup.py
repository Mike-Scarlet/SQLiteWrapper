from setuptools import setup, find_packages

setup(
  name="SQLiteWrapper",
  version="1.0.0",
  author="mike_scarlet",
  author_email="mike_scarlet@126.com",
  description="a simple sqlite wrapper",
  # url="current not defined",
  install_requires=[],
  # packages=["python_general_lib"],
  packages=find_packages(),
  python_requires='>=3.5'
)