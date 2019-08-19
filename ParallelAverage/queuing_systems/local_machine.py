from multiprocessing import Process, set_start_method, get_start_method
from subprocess import run, STDOUT
from pathlib import Path
import os


package_path = Path(os.path.abspath(__file__)).parent.parent


def run_task(python, task_id, job_path):
    with open(job_path / f"{task_id}.out", 'w') as f:
        run(
            [
                python,
                f"{package_path}/run_task.py",
                str(task_id),
                str(job_path.resolve())
            ],
            stdout=f,
            stderr=STDOUT
        )


def submit(N_tasks, job_name, job_path, user_options):
    if get_start_method() != "spawn":
        set_start_method("spawn", force=True)

    for task_id in range(1, N_tasks + 1):
        Process(
            name=job_name + f"_{task_id}",
            target=run_task,
            args=(user_options.get("python", "python3"), task_id, job_path),
            daemon=True
        ).start()

    print(f"starting {N_tasks} local processes", job_name)


def print_job_output(job_path):
    output_files = [f for f in job_path.iterdir() if str(f).endswith(".out")]
    largest_output_file = max(output_files, key=lambda f: f.stat().st_size)
    with largest_output_file.open() as f:
        print(
            f"content of largest job output file ({largest_output_file.name}):\n"
            f"{f.read()}"
        )


def cancel_job(job_name):
    raise NotImplementedError("cancelling a process on the local machine is not yet supported. Please do manually.")
