from setuptools import setup, find_packages

setup(
    name='zmq_perf_als832',  # Replace with your package's name
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'pyzmq',
        'pydantic',
        'matplotlib',
        'seaborn',
        'pandas'
    ],
    entry_points={
        'console_scripts': [
            'zmq-perf-als832-server=zmq_perf_als832.server:main',  # Adjust module path as needed
            'zmq-perf-als832-client=zmq_perf_als832.client:main',  # Adjust module path as needed
        ],
    },
)
