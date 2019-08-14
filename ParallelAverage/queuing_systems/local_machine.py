from multiprocessing import Process, set_start_method
from subprocess import run
from pathlib import Path
import os


package_path = Path(os.path.abspath(__file__)).parent.parent


def run_task(python, task_id, job_path):
    run([
        python,
        f"{package_path}/run_task.py",
        str(task_id),
        str(job_path.resolve())
    ])


def submit(N_tasks, job_name, job_path, user_options):
    set_start_method("spawn")

    for task_id in range(1, N_tasks + 1):
        Process(
            name=job_name + f"_{task_id}",
            target=run_task,
            args=(user_options.get("python", "python3"), task_id, job_path),
            daemon=True
        ).start()
