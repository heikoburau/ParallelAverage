from pathlib import Path
from subprocess import run
import os


package_path = Path(os.path.abspath(__file__)).parent.parent
config_path = Path.home() / ".config/ParallelAverage"


def submit(N_tasks, job_name, job_path, user_options):
    options = {
        "array": f"1-{N_tasks}",
        "job-name": job_name,
        "chdir": str(job_path.resolve()),
    }
    options.update(user_options)

    options_str = ""
    for name, value in options.items():
        name = name.replace('_', '-')
        if len(name) == 1:
            options_str += f"#SBATCH -{name} {value}\n"
        else:
            options_str += f"#SBATCH --{name}={value}\n"

    template_file_name = "job_script_slurm.template"
    template_file = config_path / template_file_name
    if not template_file.exists():
        template_file = package_path / "queuing_systems" / template_file_name

    with (template_file).open('r') as f:
        job_script_slurm = f.read().format(slurm_options=options_str)

    with (job_path / "job_script_slurm.sh").open('w') as f:
        f.write(job_script_slurm)

    run([
        "sbatch",
        f"{job_path}/job_script_slurm.sh",
        f"{package_path}/run_task.py",
        str(Path(".").resolve())
    ])
