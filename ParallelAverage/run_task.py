"""
This file executes a single task. It should not be aware of any other tasks belonging to the same job.
It's responsibility is to execute a specific amount of runs and to (optionally) compute their average.
In addition it may also want to save each run's result in a task-related file.
A task decides on its own the run_ids it will manage and when it's done.

Command-line arguments
======================

1: task id
2: working directory of job
"""


import os
import sys
import json
import dill
import pickle
import time as time_mod
import random
from collections import defaultdict
from itertools import product
from pathlib import Path
import traceback
from ParallelAverage import Dataset, SimpleFlock, volume, NumpyEncoder


task_id = int(sys.argv[1])
job_dir = Path(sys.argv[2])
data_dir = job_dir / "data_output"
data_dir.mkdir(exist_ok=True)
input_dir = job_dir / "input"


to_be_averaged = lambda i: average_results is not None and (average_results == 'all' or i in average_results)


with open(input_dir / "run_task_arguments.json", 'r') as f:
    parameters = json.load(f)
    job_name = parameters["job_name"]
    N_runs = parameters["N_runs"]
    N_tasks = parameters["N_tasks"]
    average_results = parameters["average_results"]
    save_interpreter_state = parameters["save_interpreter_state"]
    dynamic_load_balancing = parameters["dynamic_load_balancing"]
    N_static_runs = parameters["N_static_runs"]
    keep_runs = parameters["keep_runs"]
    encoding = parameters["encoding"]

with open(input_dir / "run_task.d", 'rb') as f:
    run_task = dill.load(f)

function = run_task["function"]
args = run_task["args"]
kwargs = run_task["kwargs"]

os.environ["JOB_NAME"] = job_name

if save_interpreter_state:
    dill.load_session(str(input_dir / "session.pkl"))


def linear_run_ids():
    if dynamic_load_balancing:
        yield from range(task_id - 1, N_static_runs, N_tasks)
        while True:
            chunk = None
            while chunk is None:
                try:
                    with SimpleFlock(str(input_dir / "chunks_lock")):
                        with open(input_dir / "chunks.json", 'r+') as f:
                            chunks = json.load(f)
                            if not chunks:
                                return

                            chunk = chunks.pop()
                            f.seek(0)
                            json.dump(chunks, f)
                            f.truncate()
                except Exception:
                    time_mod.sleep(0.5 + 0.5 * random.random())
                    chunk = None

            yield from range(*chunk)
    else:
        yield from range(task_id - 1, volume(N_runs), N_tasks)


def run_ids():
    if isinstance(N_runs, int):
        yield from (repr(run_id) for run_id in linear_run_ids())
    else:
        ranges = [range(n_i) for n_i in N_runs]
        run_id_list = list(product(*ranges))
        for linear_run_id in linear_run_ids():
            yield repr(run_id_list[linear_run_id])


def execute_run(run_id):
    global error_message

    os.environ["RUN_ID"] = run_id
    try:
        result = function(*args, **kwargs)
    except Exception as e:
        exc_type, exc_value, exc_tb = sys.exc_info()
        tb = traceback.format_exception(exc_type, exc_value, exc_tb)
        error_message = "".join([tb[0]] + tb[2:])
        print(error_message)
        return None

    try:
        with open(job_dir / "progress.txt", "a") as f:
            f.write(run_id + "\n")
    except Exception:
        print("Error while writing to progress.txt")

    if not isinstance(result, (list, tuple)):
        result = [result]

    return result


def polish(x):
    if isinstance(x, (list, tuple)) and len(x) == 1:
        x = x[0]
    return x


def dump_result_of_single_run(run_id, result):
    runs_of_task = data_dir / f"{task_id}_raw_results.{encoding}"
    runs_of_task.touch()
    with open(runs_of_task, 'r' if encoding == "json" else 'rb') as f:
        if runs_of_task.stat().st_size == 0:
            runs = {}
        else:
            if encoding == "json":
                runs = json.load(f)
            elif encoding == "pickle":
                runs = pickle.load(f)

    runs[run_id] = polish(result)

    with open(runs_of_task, 'w' if encoding == "json" else 'wb') as f:
        if encoding == "json":
            json.dump(runs, f, indent=2, cls=NumpyEncoder)
        elif encoding == "pickle":
            pickle.dump(runs, f)


def dump_task_results(done, throttle):
    global last_dump_timestamp

    if throttle and time_mod.time() - last_dump_timestamp < 30:
        return

    task_file = data_dir / f"{task_id}_task_output.json"

    with SimpleFlock(str(task_file) + ".lock"):
        with open(task_file, 'w') as f:
            json.dump(
                {
                    "done": done,
                    "successful_runs": successful_runs,
                    "failed_runs": failed_runs,
                    "error_message": {
                        "run_id": failed_runs[-1] if failed_runs else -1,
                        "message": error_message
                    },
                    "raw_results_map": {run_id: task_id for run_id in successful_runs} if keep_runs else None,
                    "task_result": [
                        task_result[i].to_json() if isinstance(task_result[i], Dataset) else task_result[i]
                        for i in sorted(task_result)
                    ],
                },
                f,
                indent=2,
                cls=NumpyEncoder
            )
    last_dump_timestamp = time_mod.time()


task_result = defaultdict(lambda: Dataset())
successful_runs = []
failed_runs = []
error_message = ""
last_dump_timestamp = time_mod.time()

for run_id in run_ids():
    run_result = execute_run(run_id)
    if run_result is None:
        failed_runs.append(run_id)
    else:
        successful_runs.append(run_id)
        if keep_runs:
            dump_result_of_single_run(run_id, run_result)

        if average_results is not None:
            for i, r in enumerate(run_result):
                if to_be_averaged(i):
                    task_result[i].add_sample(r)
                else:
                    task_result[i] = r

        dump_task_results(done=False, throttle=True)

dump_task_results(done=True, throttle=False)
