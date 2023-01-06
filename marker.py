import click
import os
import csv
import functools
import json
from collections import defaultdict


def trim_executable_name(executable_name):
    exe_parts = executable_name.split(",")
    exe_path = exe_parts[0]
    exe_basename = exe_path.split("/")[-1]
    return exe_basename


def dict_reduce(results, reduction, key=None, metric=None):
    final_results = {}

    for result in results:
        if key is not None and metric is not None:
            tempresults = final_results.get(result[key], [])
            tempresults.append(result[metric])
            final_results[result[key]] = tempresults
        else:
            for key in result.keys():
                tempresults = final_results.get(key, [])
                tempresults.append(result[key])
                final_results[key] = tempresults

    for key, value in final_results.items():
        valueafter = functools.reduce(reduction, value)
        final_results[key] = valueafter

    return final_results


@click.command()
@click.option('--basedir', required=True, help='The base directory to start looking for submissions')
def run(basedir):
    # Search for directories
    submissions = os.scandir(basedir)

    submissions = [x for x in submissions if os.path.isdir(x)]
    
    # Establish global minimum runtimes
    min_runtimes = {}
    max_runtimes = {}

    for submission in submissions:
        subdirs = list(os.scandir(submission))
        if len(subdirs) == 1:
            submission = subdirs[0]
        results_file = os.path.join(submission, "iresults.csv")
        try:
            existing_results = None
            with open(results_file) as ifile:
                reader = csv.DictReader(ifile)
                existing_results = list(reader)

            for r in existing_results:
                r['executable'] = trim_executable_name(r['executable'])
                r['runtime'] = float(r['runtime'])
            
            final_results = dict_reduce(existing_results,
                                        key="executable",
                                        metric="runtime",
                                        reduction=min)
            

            min_runtimes = dict_reduce([final_results, min_runtimes],
                                       reduction=min)
            max_runtimes = dict_reduce([final_results, max_runtimes],
                                       reduction=max)
        except FileNotFoundError as e:
            continue

    for submission in submissions:
        subdirs = list(os.scandir(submission))
        if len(subdirs) == 1:
            submission = subdirs[0]
        results_file = os.path.join(submission, "iresults.csv")
        try:
            existing_results = None
            with open(results_file) as ifile:
                reader = csv.DictReader(ifile)
                existing_results = list(reader)

            r_by_thread = defaultdict(dict)
            for r in existing_results:
                r['executable'] = trim_executable_name(r['executable'])
                r['runtime'] = float(r['runtime'])
                r['threads'] = int(r['threads'])
                executable = r['executable']
                threads = r['threads']
                runtime = r['runtime']
                r_by_thread[executable][threads] = runtime
            
            final_results = dict_reduce(existing_results,
                                        key="executable",
                                        metric="runtime",
                                        reduction=min)
            
            for k, v in final_results.items():
                max_r = max_runtimes[k]
                min_r = min_runtimes[k]
                final_results[k] = round((max_r-v)/(max_r-min_r)*5, 1)
            min_runtime_marks = sum(final_results.values())
            
            m_by_thread = {}
            for e in r_by_thread.keys():
                t = 1
                while t<32:
                    original_value = m_by_thread.get(t, 0)
                    r_by_thread[e].keys()
                    if t in r_by_thread[e].keys() and 2*t in r_by_thread[e].keys():
                        m_by_thread[t] = round(((r_by_thread[e][t]/r_by_thread[e][2*t])/2)*2.5 + original_value, 1)
                    else:
                        m_by_thread[t] = original_value
                    t *= 2
            

            marks = {'min_runtime': min_runtime_marks,
                     'scalability': m_by_thread}
            
            marks_file = os.path.join(submission, "marks.json")
            with open(marks_file, 'w') as f:
                json.dump(marks, f)
            

        except FileNotFoundError as e:
            continue
            






if __name__=="__main__":
    run()
