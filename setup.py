from setuptools import setup

__version__ = '0.0.2'


setup(
    name='ParallelAverage',
    version=__version__,
    author="Heiko Burau",
    author_email="burau@pks.mpg.de",
    packages=["ParallelAverage", "ParallelAverage/queuing_systems", "ParallelAverage/legacy"],
    data_files=[
        (
            "ParallelAverage/queuing_systems", [
                "ParallelAverage/queuing_systems/job_script_slurm.template",
            ]
        ),
        (
            "ParallelAverage/legacy", [
                "ParallelAverage/legacy/average_collector.sh"
            ]
        )
    ],
    install_requires=['numpy', 'dill'],
    zip_safe=False,
    license="GNU GENERAL PUBLIC LICENSE Version 3",
    url="https://github.com/Heikman/ParallelAverage"
)
