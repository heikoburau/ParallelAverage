ParallelAverage
===============

Facilitates the full workflow of high performance computing (HPC) for Python Users, preferably in an interactive environment such as the [jupyter-notebook](https://jupyter.org/).

It basically converts a Python function call that performs the calculation into a [SLURM](https://slurm.schedmd.com/) batch script, sends it to the cluster, and finally stores all the results of the job in a database.
Whenever the same function is called again with the same arguments, the pre-calculated results are returned immediately from the database.
From the user's perspective, all functionality is contained in a single function call.

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

If different parameters are used, a new job will be submitted and a new entry will be put in the database. 

Features
--------

- Any custom keyword arguments are put as SLURM parameters within internal batch file.
- Basic statistical functionality included, such as average, variance, stat. error, ...
- Intermediate results are available at any point in time.
- Fine control over job management with the help of additional decorators, such as `@dont_submit, @do_submit, @cancel_job, ...`.
- Supports both JSON and binary output data formats.
- Supports re-submission of broken or partly failed jobs.
- Fallback for utilizing only the local machine by spawning multiple processes instead of submitting a job.

ParallelAverage - Browser
-------------------------

ParallelAverage comes with its own [web based User interface](https://heikoburau.github.io/ParallelAverage-browser) for an intuitive browsing of its databases. It helps to keep track of a growing number of function calls / SLURM-jobs.
Check out the repository [here](https://github.com/heikoburau/ParallelAverage-browser).

![pae_sm](https://user-images.githubusercontent.com/5159590/146452679-5cc9b054-3767-483e-bca2-83eadf958bbb.png)




