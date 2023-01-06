import codegrade
import csv
import requests
import os
import zipfile
import click
import subprocess
import shlex
import schedule
import portalocker
import yaml
import time
from mako.template import Template
from portalocker import LockException
from pathlib import Path


def dir_exists(path):
    if os.path.exists(path):
        assert(os.path.isdir(path))
    else:
        os.mkdir(path)


def download_file(url, filename):
    r = requests.get(url, stream=True)
    with open(filename,'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
         if chunk:
             f.write(chunk)


def unzip_file(filename, directory_to_extract_to):
    with zipfile.ZipFile(filename, 'r') as zip_ref:
        zip_ref.extractall(directory_to_extract_to)


def run_command(command, cwd, shell=False, output_file=None):
    print("Executing command `%s`"%command)
    if command is None:
        return
    c = shlex.split(command)
    p = subprocess.run(c, cwd=cwd, shell=shell, check=True, capture_output=True)

    if output_file is not None:
        with open(output_file, 'w') as file:
            file.write(command)
            file.write("\n")
            file.write(p.stdout)
            file.write("\n")
            file.write(p.stderr)
    return p.returncode


def download_submissions(username, password, tenant, assignment_id, basepath):
    with codegrade.login(username=username, password=password, tenant=tenant) as client:
        submissions = client.assignment.get_all_submissions(assignment_id=assignment_id, latest_only=True)
        list_of_ids = []
        for submission in submissions:
            detailed_submission = client.submission.get(submission_id=submission.id, type="zip")
            file_to_download = detailed_submission.url
            saved_file_name = os.path.join(basepath, "%s.zip" % submission.user.id)
            list_of_ids.append({'user': submission.user.id, 'file': submission.id,
                                'username': submission.user.username})
            download_file(file_to_download, saved_file_name)
    return list_of_ids


def filter_new_submissions(submissions, submission_record_filename="records.csv"):
    if not os.path.exists(submission_record_filename):
        return submissions

    with open(submission_record_filename) as ifile:
        reader = csv.DictReader(ifile)
        existing_results = list(reader)

    excluded_submissions = []
    for submission in submissions:
        for existing_submission in existing_results:
            if str(existing_submission['user']) == str(submission['user']) and \
                str(existing_submission['file']) == str(submission['file']):
                excluded_submissions.append(submission)
                break

    filtered_submissions = [x for x in submissions if x not in excluded_submissions]

    return filtered_submissions         


def extract_submissions(submissions, frompath, topath):
    outgoing_submissions = []
    for submission_dict in submissions:
        submission_user_id = submission_dict['user']
        directory_to_extract_to = os.path.join(topath, str(submission_user_id))
        saved_file_name = os.path.join(frompath, "%s.zip" % submission_user_id)
        unzip_file(saved_file_name, directory_to_extract_to)

        while len(os.listdir(directory_to_extract_to)) == 1:
            directory_to_extract_to = os.path.join(directory_to_extract_to, os.listdir(directory_to_extract_to)[0])
        submission_dict['path'] = directory_to_extract_to
        outgoing_submissions.append(submission_dict)
    return outgoing_submissions


def compile_submissions(submissions, compile_commands, artifacts_path, setup_commands):
    for command in setup_commands:
        run_command(command, cwd=artifacts_path, shell=True)

    successful_submissions = []
    for submission_dict in submissions:
        submission_dir = submission_dict['path']
        rc = 0
        for command in compile_commands:
            command = command.format_map({'artifacts_path': artifacts_path})
            rc += run_command(command, cwd=submission_dir)
        if rc == 0:
            successful_submissions.append(submission_dict)
    return successful_submissions


def get_artifacts(artifacts_repo, artifacts_path):
    artifacts_dir = os.path.join(artifacts_path, artifacts_repo.split("/")[-1].split(".")[0])

    if not os.path.exists(artifacts_dir):
        run_command("git clone %s" % artifacts_repo, cwd=artifacts_path)
    else:
        run_command("git pull origin main", cwd=artifacts_dir)
    return artifacts_dir


def prepare_slurm_file(submission_dict, submission_template, target_dir, artifacts_path, leaderboard_repo,
                        update_frequency):
    submission_template_full_path = os.path.join(artifacts_path, submission_template)
    slurm_file_template = Template(filename=submission_template_full_path)

    submission_file_string = slurm_file_template.render(submission_dict=submission_dict, artifacts_path=artifacts_path,
                                                        leaderboard_repo=leaderboard_repo,
                                                        update_frequency=update_frequency)
    target_slurm_filename = os.path.join(target_dir, 'p%s.sh' % submission_dict['user'])
    with open(target_slurm_filename, "w") as text_file:
        text_file.write(submission_file_string)
    return target_slurm_filename


def call_slurm(slurm_file, context_dir):
    run_command("sbatch --nice %s" % slurm_file, cwd=context_dir)


def load_config(config_file):
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)

    return config


def record(submissions, submission_record_filename="records.csv"):
    if len(submissions) == 0:
        return

    exists = os.path.exists(submission_record_filename)
    lock = portalocker.Lock(submission_record_filename)

    existing_results = []
    with lock:
        with open(submission_record_filename, mode="a") as ofile:
                writer = csv.DictWriter(ofile, fieldnames=submissions[0].keys())
                if not exists:
                    writer.writeheader()
                for row in submissions:
                    writer.writerow(row)

def call_submission_processor(submission, submission_processor, artifacts_path):
    submission_processor = submission_processor.format_map({'submission_dir': submission['path'],
                                                            'submission_id': submission['user'],
                                                            'artifacts_path': artifacts_path})
    run_command(submission_processor, cwd=submission['path'])


def run(config):
    basepath = config['basepath']
    username = config['username']
    password = config['password']
    tenant = config['tenant']
    assignment_id = config['assignment_id']
    artifacts_repo = config['artifacts_repo']
    setup_commands = config['setup_commands']
    leaderboard_repo = config['leaderboard_repo']
    update_frequency = int(config['update_frequency'])
    submission_processor = config['submission_processor']

    basepath = os.path.abspath(basepath)
    dir_exists(basepath)

    submissions_path = os.path.join(basepath, "submissions")
    dir_exists(submissions_path)
    submissions_download_path = os.path.join(submissions_path, "downloaded")
    dir_exists(submissions_download_path)
    print("Downloading submissions...")
    submissions = download_submissions(username, password,
                                       tenant, assignment_id,
                                       submissions_download_path)

    submissions = filter_new_submissions(submissions)

    submissions_extract_path = os.path.join(submissions_path, "extracted")
    dir_exists(submissions_extract_path)
    print("Extracting...")
    submissions = extract_submissions(submissions, submissions_download_path,
                                      submissions_extract_path)

    artifacts_path = os.path.join(basepath, "artifacts")
    dir_exists(artifacts_path)
    artifacts_path = get_artifacts(artifacts_repo, artifacts_path)

    for command in setup_commands:
        command = command.format_map({'artifacts_path': artifacts_path})
        run_command(command, cwd=artifacts_path)

    record(submissions,
           submission_record_filename=os.path.join(basepath,
                                                   "submission-records.csv"))

    for s in submissions:
        call_submission_processor(s, submission_processor, artifacts_path)

    print("All done. (Probably) going to sleep...")


@click.command()
@click.option('--config-file', default="config.yaml",
              help='The yaml config file')
@click.option('--lock-file', default="codegrade.running",
              help='The filename used to track whether a process is running')
@click.option('--unlock-file', default="codegrade.stop",
              help='The filename used to ask the running process to shut down')
def looper(config_file, lock_file, unlock_file):
    lock = portalocker.Lock(lock_file)
    lock_acquired = False
    try:
        file_lock = lock.acquire(fail_when_locked=True) # noqa
        lock_acquired = True

        config = load_config(config_file)

        def run_closure():
            run(config)

        auto_update = bool(config['auto_update'])
        update_frequency = int(config['update_frequency'])
        assignment_deadline = config['assignment_deadline']

        run_closure()

        if auto_update:
            schedule.every(update_frequency).\
                hours.until(assignment_deadline).do(run_closure)

        while auto_update:
            if os.path.exists(unlock_file):
                print("Stop file `%s` exists." % unlock_file)
                print("I'm being asked to stop. Seeya later")
                break
            schedule.run_pending()
            n = schedule.idle_seconds()
            if n is None:
                # no more jobs
                break
            elif n > 0:
                # sleep exactly the right amount of time
                time.sleep(n)

    except LockException:
        print("Process already running. Send a kill signal?")
        Path(unlock_file).touch()
    finally:
        if lock_acquired:
            os.remove(lock_file)
            lock.release()


if __name__ == "__main__":
    looper()
