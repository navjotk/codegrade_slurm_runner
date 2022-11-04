import codegrade
import requests
import os
import zipfile
import click
import subprocess
import shlex
import schedule
from crontab import CronTab
from mako.template import Template


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

def run_command(command, cwd, shell=False):
    print("Executing command `%s`"%command)
    c = shlex.split(command)
    p = subprocess.Popen(c, cwd=cwd, shell=shell)
    return p.wait()


def download_submissions(basepath):
    with codegrade.login(username=username, password=password, tenant=tenant) as client:
        submissions = client.assignment.get_all_submissions(assignment_id=assignment_id, latest_only=True)
        list_of_ids = []
        for submission in submissions:
            detailed_submission = client.submission.get(submission_id=submission.id, type="zip")
            file_to_download = detailed_submission.url
            saved_file_name = "%s/%s.zip" % (basepath, submission.user.id)
            list_of_ids.append(submission.user.id)
            download_file(file_to_download, saved_file_name)
    return list_of_ids


def filter_new_submissions(submissions):
    return submissions         


def extract_submissions(submissions, frompath, topath, append_path="top"):
    paths = []
    for submission in submissions:
        directory_to_extract_to = "%s/%s" % (topath, submission)
        saved_file_name = "%s/%s.zip" % (frompath, submission)
        unzip_file(saved_file_name, directory_to_extract_to)
        directory_to_extract_to = "%s/%s" % (directory_to_extract_to, append_path)
        paths.append(directory_to_extract_to)
    return paths


def compile_submissions(submissions, compile_commands, artifacts_path, setup_commands):
    for command in setup_commands:
        run_command(command, cwd=artifacts_path, shell=True)
    
    successful_submissions = []
    for submission in submissions:
        rc = 0
        for command in compile_commands:
            command = command.format_map({'artifacts_path': artifacts_path})
            rc += run_command(command, cwd=submission)
        if rc == 0:
            successful_submissions.append(submission)
    return successful_submissions


def get_artifacts(artifacts_repo, artifacts_path):
    artifacts_dir = "%s/%s" % (artifacts_path, artifacts_repo.split("/")[-1].split(".")[0])

    if not os.path.exists(artifacts_dir):
        run_command("git clone %s" % artifacts_repo, cwd=artifacts_path)
    else:
        run_command("git pull origin main", cwd=artifacts_dir)
    return artifacts_dir


def prepare_slurm_file(submissions, submission_template, target_dir, artifacts_path, leaderboard_repo,
                        update_frequency):
    submission_template_full_path = "%s/%s" % (artifacts_path, submission_template)
    slurm_file_template = Template(filename=submission_template_full_path)
    submission_ids = [x.split("/")[-2] for x in submissions]
    submissions = zip(submission_ids, submissions)
    submission_file_string = slurm_file_template.render(submissions=submissions, artifacts_path=artifacts_path,
                                                        leaderboard_repo=leaderboard_repo,
                                                        update_frequency=update_frequency)
    target_slurm_filename = "%s/%s" % (target_dir, submission_template)
    with open(target_slurm_filename, "w") as text_file:
        text_file.write(submission_file_string)
    return target_slurm_filename


def call_slurm(slurm_file, context_dir):
    run_command("sbatch %s" % slurm_file, cwd=context_dir)


def schedule_call_me(update_frequency):
    cron = CronTab(user=True)
    myname = sys.argv[0]
    file_path = os.path.realpath(__file__)
    command = "python %s > %s/output.log" % (myname, file_path)
    timedelta = datetime.timedelta(hours=update_frequency)
    job = cron.new(command=command)
    job.setall(datetime.datetime.now() + timedelta)
    cron.write()
    


def run():
    global basepath
    global submission_template

    basepath = os.path.abspath(basepath)
    dir_exists(basepath)

    submissions_path = "%s/submissions" % basepath
    dir_exists(submissions_path)
    submissions_download_path = "%s/downloaded" % submissions_path
    dir_exists(submissions_download_path)
    submissions = download_submissions(submissions_download_path)
    
    submissions = filter_new_submissions(submissions)
    
    submissions_extract_path = "%s/extracted" % submissions_path
    dir_exists(submissions_extract_path)
    submissions = extract_submissions(submissions, submissions_download_path,
                                      submissions_extract_path)
    
    artifacts_path = "%s/artifacts" % basepath
    dir_exists(artifacts_path)
    artifacts_path = get_artifacts(artifacts_repo, artifacts_path)
    
    submissions = compile_submissions(submissions, compile_commands, artifacts_path, setup_commands)

    if len(submissions) > 0:
        slurm_file_path = "%s/slurm" % basepath
        dir_exists(slurm_file_path)
        slurm_file = prepare_slurm_file(submissions, submission_template, slurm_file_path, artifacts_path, leaderboard_repo, update_frequency)
        call_slurm(slurm_file, slurm_file_path)
    
    if auto_update:
        schedule_call_me(update_frequency)

    

if __name__ == "__main__":
    run()



