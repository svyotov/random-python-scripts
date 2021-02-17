#!/usr/bin/env python3
"""
Slims down AWS lambda python deployments - speed up deployments, reduce cost, cleaner code
MIT licence
Author: svyotov
"""
import re
import sys
import os
import subprocess
import argparse
import logging
from logging.config import dictConfig
import shutil
import zipfile

_globals = {"pip_files": "pip-packaged-files.txt",
            "anti_req": "anti-requirements.txt",
            "zip_target": "requirements.zip"}

logging_config = dict(
    version=1,
    formatters={
        'f': {'format':
              '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'}
    },
    handlers={
        'h': {'class': 'logging.StreamHandler',
              'formatter': 'f',
              'level': logging.DEBUG}
    },
    root={
        'handlers': ['h'],
        'level': logging.DEBUG,
    },
)

dictConfig(logging_config)

logger = logging.getLogger()


def unzip_custom_requirements()-> str:
    """Python code for custom resources unzip in lambda
    """
    return """#!/usr/bin/env python3
# source: https://github.com/UnitedIncome/serverless-python-requirements/blob/master/unzip_requirements.py
# Licence when copied: MIT License
import os
import shutil
import sys
import zipfile

pkgdir = '/tmp/sls-py-req'

# We want our path to look like [working_dir, serverless_requirements, ...]
sys.path.insert(1, pkgdir)

if not os.path.exists(pkgdir):
    tempdir = '/tmp/_temp-sls-py-req'
    if os.path.exists(tempdir):
        shutil.rmtree(tempdir)

    default_lambda_task_root = os.environ.get('LAMBDA_TASK_ROOT', os.getcwd())
    lambda_task_root = os.getcwd() if os.environ.get('IS_LOCAL') == 'true' else default_lambda_task_root
    zip_requirements = os.path.join(lambda_task_root, 'requirements.zip')

    zipfile.ZipFile(zip_requirements, 'r').extractall(tempdir)
    os.rename(tempdir, pkgdir)  # Atomic
"""

class PIPInstall(Exception):
    """Failed to install pip requirements"""


class InvalidRegExp(Exception):
    """Failed to process invalid regex"""


def get_slim_pattern(wd: str):
    """[summary]

    Args:
        wd (str): the lambda python project working directory

    Returns:
        [type]: [description]
    """
    global _globals
    sp, ok = read_file_lines(wd, _globals["anti_req"])
    sp = sp.union(set([re.sub(r'/\*$', "-*", x)
                       for x in sp if x.endswith("/*")]))
    return [re_compile(x) for x in sp]


def re_compile(pattern):
    """[summary]

    Args:
        pattern ([type]): [description]

    Raises:
        InvalidRegExp: [description]

    Returns:
        [type]: [description]
    """
    pattern = pattern.replace("*", ".*")
    try:
        regex = re.compile('^' + pattern + '$', re.IGNORECASE)
    except re.error as e:
        raise InvalidRegExp(
            'There was a problem processing the pattern({}) provided'.format(pattern))
    return regex


def read_file_lines(wd: str, file_name: str):
    """[summary]

    Args:
        wd (str): the lambda python project working directory
        file_name (str): [description]

    Returns:
        [type]: [description]
    """
    full_path = '{}/{}'.format(wd, file_name)
    if not os.path.exists(full_path):
        return [], False
    with open(full_path) as f:
        data = set(f.read().splitlines())
    return data, True


def get_delete_list(wd: str, all_files: list):
    """[summary]

    Args:
        wd (str): the lambda python project working directory
        all_files (list): [description]

    Returns:
        [type]: [description]
    """
    slim_patterns = get_slim_pattern(wd)
    bad_files = {}
    for pattern in slim_patterns:
        for file in all_files:
            if pattern.match(file):
                bad_files[file] = True
    logger.info("bad files {} files ...".format(len(bad_files.keys())))
    return list(bad_files.keys())


def slim_down_package(wd: str, pip_tmp_dir: str, all_files: list):
    """[summary]

    Args:
        wd (str): the lambda python project working directory
        all_files (list): [description]
    """
    logger.info(
        "starting packages slim down for {} files ...".format(len(all_files)))
    files_to_delete = get_delete_list(wd, all_files)
    for file in files_to_delete:
        file_path = os.path.join(wd, pip_tmp_dir, file)
        if ok_to_delete(wd, file_path):
            os.remove(file_path)
    return list_all_files(os.path.join(wd, pip_tmp_dir))


def ok_to_delete(wd: str, file_path: str):
    ok = _ok_to_delete(wd, file_path)
    logger.info(
        "ok={} to delete file({})".format(ok, file_path))
    return ok


def _ok_to_delete(wd: str, file_path: str):
    if not os.path.exists(file_path):
        return False
    parent_dir = os.path.dirname(
        file_path) if os.path.isfile(file_path) else file_path
    if not parent_dir.startswith(wd):
        logger.debug(
            "ok=False - ({}) not subfolder of ({})".format(parent_dir, wd))
        return False
    if file_path == wd:
        logger.debug(
            "ok=False - cannot delete root dir".format(parent_dir, wd))
        return False
    return True


def list_all_files(dir_name: str, recursive: bool = True):
    """[summary]

    Args:
        dir_name (str): [description]
        recursive (bool, optional): [description]. Defaults to True.

    Returns:
        [type]: [description]
    """
    files = _list_all_files(dir_name, recursive)
    new_files = [remove_prefix(x, dir_name) for x in files]
    return new_files


def remove_prefix(self: str, prefix: str) -> str:
    if self.startswith(prefix):
        return self[len(prefix):]
    else:
        return self[:]


def _list_all_files(dir_name: str, recursive: bool):
    list_of_files = os.listdir(dir_name)
    complete_file_list = list()
    for file in list_of_files:
        complete_path = os.path.join(dir_name, file)
        if os.path.isdir(complete_path) and recursive:
            complete_file_list = complete_file_list + \
                _list_all_files(complete_path, recursive)
        else:
            complete_file_list.append(complete_path)
    return complete_file_list


def install_pip_requirements(wd: str, pip_tmp_dir: str):
    """[summary]

    Args:
        wd ([type]): [description]

    Raises:
        PIPInstall: [description]
    """
    p_dir = os.path.join(wd, pip_tmp_dir)
    clean_up_pip(wd, pip_tmp_dir)
    logger.info("starting pip requirements installation in({})".format(p_dir))
    result = subprocess.run(["pip", "install", "--upgrade", "-t", pip_tmp_dir,
                             "-r", "requirements.txt"], capture_output=True, check=True, cwd=wd)
    if result.returncode != 0:
        raise PIPInstall(result.stderr)
    return list_all_files(p_dir)


def finalize_packages_list_as_files(wd: str, pip_tmp_dir: str, files: list):
    """[summary]

    Args:
        wd (str): the lambda python project working directory
        files (list): [description]
    """
    global _globals
    logger.info(
        "starting package finalization as zip in({}) for: {} files".format(os.path.join(wd, pip_tmp_dir), len(files)))
    with open(os.path.join(wd, _globals["pip_files"]), 'w') as f:
        for file_name in list_all_files(os.path.join(wd, pip_tmp_dir), recursive=False):
            if len(file_name) > 0:
                logger.info("- ::{}".format(file_name))
                f.write(file_name)
                f.write('\n')
    for file_name in files:
        new_file_name = os.path.join(wd, file_name)
        old_file_name = os.path.join(wd, pip_tmp_dir, file_name)
        copy_file(old_file_name, new_file_name)
    return clean_up_pip(wd, pip_tmp_dir)


def copy_file(src_path, dest_path):
    parent_dir = os.path.dirname(dest_path)
    if not os.path.exists(src_path):
        return
    if not os.path.exists(parent_dir):
        os.makedirs(parent_dir)
    return shutil.copy(src_path, dest_path)


def clean_up_pip(wd: str, pip_tmp_dir: str):
    """[summary]

    Args:
        wd (str): the lambda python project working directory
        pip_tmp_dir (str): [description]
    """
    dir_path = os.path.join(wd, pip_tmp_dir)
    if ok_to_delete(wd, dir_path):
        logger.info("delering build path ({}) ...".format(dir_path))
        return shutil.rmtree(dir_path)
    return 0


def finalize_packages_list_as_zip(wd: str, pip_tmp_dir: str, files: list):
    """Compress the local files to a zip

    Args:
        wd (str): the lambda python project working directory
        pip_tmp_dir (str): [description]
        files (list): [description]
    """
    global _globals
    logger.info(
        "starting package finalization as files in({}) for: {} files".format(os.path.join(wd, pip_tmp_dir), len(files)))
    zip_obj = zipfile.ZipFile(os.path.join(wd, _globals["zip_target"]), 'w', compression=zipfile.ZIP_DEFLATED)
    zip_obj.writestr(zinfo_or_arcname="unzip_custom_requirements.py", data=unzip_custom_requirements()) # runs first, allows local override
    for file_name in files:
        full_path = os.path.join(wd, pip_tmp_dir, file_name)
        if os.path.exists(full_path):
            zip_obj.write(filename=full_path, arcname=file_name)
    return clean_up_pip(wd, pip_tmp_dir)


def prepare_lambda_package(wd: str, output_format: str):
    """Preparation steps such as pip install requirements

    Args:
        wd (str): the lambda python project working directory
    """
    logger.info(
        "starting lambda_package finalization for({}) as ({}) format".format(wd, output_format))
    pip_tmp_dir = ".dist/"
    all_new_files = install_pip_requirements(wd, pip_tmp_dir)
    kept_files = slim_down_package(wd, pip_tmp_dir, all_new_files)
    if output_format == "files":
        return finalize_packages_list_as_files(wd, pip_tmp_dir, kept_files)
    return finalize_packages_list_as_zip(wd, pip_tmp_dir, kept_files)


def clean_up_old_runs(wd: str):
    """Ensure old builds are removed to keep the working directory clean from temp files

    Args:
        wd (str): the lambda python project working directory
    """
    global _globals
    files_to_delete, ok = read_file_lines(wd, _globals["pip_files"])
    if len(files_to_delete) > 0:
        files_to_delete.add(_globals["pip_files"])
    logger.info("cleaning up old package runs in({}): {} files/dirs to be removed".format(
        wd, len(files_to_delete)))
    for file in files_to_delete:
        file_path = os.path.join(wd, file)
        if ok_to_delete(wd, file_path):
            logger.debug("deleted: {}".format(file_path))
            shutil.rmtree(file_path)


def main():
    """Slims down AWS lambda python deployments
    """
    parser = argparse.ArgumentParser(
        prog='Slim down a Python program for lambda packaging')
    parser.add_argument('--wd', type=str, required=True,
                        help='the working directory for the lambda function code')
    parser.add_argument('--clean-up', type=bool, default=False,
                        required=False, help='clean up old runs to revert changes to wd')
    parser.add_argument('--output-format', type=str, default="zip",
                        required=False, choices=['files', 'zip'], help='produce the final output as files or as a zip (default: %(default)s)')
    args = parser.parse_args()
    try:
        clean_up_old_runs(args.wd)
        ret_code = prepare_lambda_package(args.wd, args.output_format)
        logger.info("all done with exit code {} ...".format(ret_code))
        return(ret_code)
    except PIPInstall as exc:
        logger.error(
            "Failed to install the pip requirements. The reason is: %s", exc)
        return(1)
    except OSError as e:
        logger.error("Error: %s - %s." % (e.filename, e.strerror))
        return(1)
    except Exception as exc:
        logger.error("Unexpected error: %s - %s", exc.__class__.__name__, exc)
        return(1)
    except BaseException:
        logger.info("user stop")  # this deals with a ctrl-c
        return(1)


if __name__ == '__main__':
    sys.exit(main())
