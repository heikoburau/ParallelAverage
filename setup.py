from setuptools import setup

__version__ = '0.0.1'


setup(
    name='ParallelAverage',
    version=__version__,
    author="Heiko Burau",
    author_email="burau@pks.mpg.de",
    packages=["ParallelAverage"],
    data_files=[(
        "ParallelAverage", [
            "ParallelAverage/job_script_slurm.template",
            "ParallelAverage/average_collector.sh"
        ]
    )],
    install_requires=['numpy', 'dill'],
    zip_safe=False,
    license="GNU GENERAL PUBLIC LICENSE Version 3",
    url="https://github.com/Heikman/ParallelAverage"
)
