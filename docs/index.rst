.. ParallelAverage documentation master file, created by
   sphinx-quickstart on Sat Aug  1 15:11:46 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome
=======

ParallelAverage replaces batch scripts for queuing systems by simple python decorators. 
It wraps all effort to prepare, submit and retrieve the result of parallel SLURM-jobs into a call to the function you already have. 
Let's look at a simple example: ::

    import numpy as np
    from ParallelAverage import parallel

    @parallel(
        N_runs=10,
        N_tasks=5,
        time="00:02:00",
        mem="1G"
    )
    def my_function(L):
        """This function just draws a list of `L` random numbers 
        and returns it.
        """

        return np.random.rand(L)

By calling the function in this example, a SLURM job-array is submitted running 10 instances of `my_function` in total, using 5 SLURM-tasks in parallel: ::

    >>> my_function(L=64)
    [ParallelAverage] submitting job-array 1_my_function

After the job has been fully or partially completed, calling the function now returns the output of each completed instance: ::

    >>> outputs = my_function(L=64)
    [ParallelAverage] Warning: 4 / 10 runs are not ready yet!
    >>> outputs[0]
    array([...])

In the above example, the parameters `N_runs` and `N_tasks` are dedicated to ParallelAverage, while all others get forwarded to the queuing system.
All hardware requirements and other SLURM-related options are thus defined near the user code.

	ParallelAverage keeps track of every job ever submitted, within a database. 
	This means that a job is recognized by the name of the function plus the arguments you used to call it.
	If the same function is called again with different arguments, a new job will be submitted, otherwise the already computed output will be returned immediately from hard disk.
	By default, the job itself is placed within a sub-folder of the current working directory.

So far, that's it in a nutshell. 

Have fun trying it out!

The following additional features emerged as convenient helpers over the last few years of real-world application from multiple users in a scientific context.

Features
--------

 * Build-in averaging over all runs, provided with basic statistical measures like variance or estimated error. Therefore an early preview of the produced data is available while the job is running.
 * Re-submission of only the failed or aborted runs, thus avoiding a costly re-submission of the entire job in case of a failure.
 * Basic dynamic load balancing.
 * Emulation of a queuing system on the local machine.
 * Nice little decorators that you can put on top for fine tuning the workflow, like `@dont_submit`, `@do_submit`, `@re_submit`, `@cancel_job`, ...
 
ParallelAverage-viewer
----------------------




.. toctree::
   :maxdepth: 2
   :caption: Contents:



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
