from setuptools import setup


setup(
    name='Snowflake_Lite',
    version='0.1.1',
    packages=["snowflake_lite"],
    install_requires=[
        'pandas', 'snowflake-connector-python'])
