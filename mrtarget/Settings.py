import os
import pkg_resources as res
import mrtarget


def file_or_resource(fname=None):
    '''get filename and check if in getcwd then get from
    the package resources folder
    '''
    filename = os.path.expanduser(fname)

    resource_package = mrtarget.__name__
    resource_path = '/'.join(('resources', filename))

    if filename is not None:
        abs_filename = os.path.join(os.path.abspath(os.getcwd()), filename) \
                       if not os.path.isabs(filename) else filename

        return abs_filename if os.path.isfile(abs_filename) \
            else res.resource_filename(resource_package, resource_path)

