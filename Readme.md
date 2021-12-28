ParallelAverage
===============

Facilitates the full workflow of high performance computing (HPC) for Python Users, preferably in an interactive environment such as the [jupyter-notebook](https://jupyter.org/).

ParallelAverage converts a Python function, that performs a calculation, into a [SLURM](https://slurm.schedmd.com/) batch script, sends it to the cluster, and has all the results of the job written to the file system.
Whenever the same function is called again with the same arguments, the pre-calculated results are directly returned from the file system.
From the User's perspective, all functionality is contained in a single function call.

Example
-------

Without ParallelAverage:


```python
def weather_fluid_simulation(N_cells, t_end, other_parameters):
    # implementation...

    return forecast


# This will execute the function on the local machine:

forecast = weather_fluid_simulation(
    N_cells=[1000, 1000],
    t_end=10.0,
    other_parameters={"add_initial_random_noise": True}
)
```

Now let's turn `weather_fluid_simulation` into a SLURM-job:

```python
from ParallelAverage import parallel


@parallel(
    N_runs=500,
    N_tasks=100,
    path="/scratch/project_name",
    time="24:00:00",
    mem="16G",
    # more SLURM parameters...
)
def weather_fluid_simulation(N_cells, t_end, other_parameters):
    # implementation...

    return forecast


# At first call, this will submit an array-job to SLURM.
# At second, third, ... call, it will return the (so far) calculated result.
# When the job has finished, `forecast` will hold all results of 500 function calls (`N_runs`),
# which were computed by 100 tasks (`N_tasks`) in parallel.

forecast = weather_fluid_simulation(
    N_cells=[1000, 1000],
    t_end=10.0,
    other_parameters={"add_initial_random_noise": True}
)
```

- If different arguments are passed to `weather_fluid_simulation`, a new job is submitted and a new entry is added to an internal database. 
- When a job is submitted, a new folder below the path given by the `path` argument is created, where all data is stored in regular files and can be manually accessed if needed.
- The database, which is a single JSON file, can also be found at the path given by the `path` argument.

Features
--------

- Custom keyword arguments are translated into SLURM-parameters for an internal batch file. This way, users have full control over hardware requirements, partitions, wall time, etc.
- Basic statistical functionality such as average, variance, statistical error, ... are included.
- Intermediate results are available at any point in time. Users don't have to wait until the job has finished.
- Additional decorators such as `@dont_submit, @do_submit, @cancel_job, ...` allow convenient control of jobs.
- Supports both JSON and binary output data formats.
- Re-submission of broken or partly failed jobs.
- Fallback mode for utilizing only the local machine by spawning multiple processes instead of submitting a job.
- Basic dynamic load balancing.
- Transfers the state of the Python interpreter to the cluster thereby users can readily use global variables and packages in their code.

ParallelAverage - Browser
-------------------------

ParallelAverage comes with its own [web based User interface](https://heikoburau.github.io/ParallelAverage-browser) for an intuitive browsing of its databases. It helps users to keep track of a growing number of function calls / SLURM-jobs.
Check out the repository [here](https://github.com/heikoburau/ParallelAverage-browser).

![pae_sm](https://user-images.githubusercontent.com/5159590/146452679-5cc9b054-3767-483e-bca2-83eadf958bbb.png)




