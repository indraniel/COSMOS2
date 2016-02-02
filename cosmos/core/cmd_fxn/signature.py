import re
from ... import NOOP
import funcsigs
import os


def get_call_kwargs(cmd_fxn, tags, input_map, output_map):
    sig = funcsigs.signature(cmd_fxn)

    def gen_params():
        for keyword, param in sig.parameters.iteritems():
            if keyword in input_map:
                yield keyword, input_map[keyword]
            elif keyword in output_map:
                yield keyword, output_map[keyword]
            elif keyword in tags:
                yield keyword, tags[keyword]
            elif param.default != funcsigs._empty:
                yield keyword, param.default
            else:
                raise AttributeError(
                        '%s requires the parameter `%s`, are you missing a tag?  Either provide a default in the cmd() '
                        'method signature, or pass a value for `%s` with a tag' % (cmd_fxn, keyword, keyword))

    kwargs = {k: v.format(**tags) if isinstance(v, basestring) else v for k, v in gen_params()}
    return kwargs


import decorator


def default_prepend(execution_output_dir, task_output_dir, cd_to_task_output_dir=True):
    if task_output_dir and task_output_dir != '':
        task_output_dir = os.path.join(execution_output_dir, task_output_dir)
        mkdir = 'mkdir -p %s\n' % task_output_dir
    else:
        task_output_dir = execution_output_dir
        mkdir = ''

    return '#!/bin/bash\n' \
           'set -e\n' \
           'set -o pipefail\n' \
           'EXECUTION_OUTPUT_DIR={ex_out}\n' \
           '{mkdir}\n' \
           '\n\n'.format(ex_out=execution_output_dir,
                         mkdir=mkdir)


# def default_cmd_append(task):
#     return ''


def default_cmd_fxn_wrapper(task, stage_name, input_map, output_map, cd_to_task_output_dir=True):
    """
    WARNING this function signature is not set in stone yet and may change, replace at your own risk.

    :param task:
    :param input_map:
    :param output_map:
    :return:
    """

    def real_decorator(fxn, *args, **kwargs):
        r = fxn(*args, **kwargs)
        assert isinstance(r, basestring) or r is None, 'cmd_fxn %s did not return a str or None' % fxn
        if r is None:
            return None
        else:
            return default_prepend(task.execution.output_dir, task.output_dir, cd_to_task_output_dir=cd_to_task_output_dir) + r

    return decorator.decorator(real_decorator)
