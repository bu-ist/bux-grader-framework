import hashlib


def class_imported_from(cls, modules):
    """ Check whether or not a class was defined in the passed module list

        :param cls: a new-style Python class
        :param list modules: a list of modules to check for class def in
        :retval: ``True`` if class was imported from one of the
                 provided modules. ``False`` otherwise.
    """
    contained = [mod for mod in modules if cls.__module__.startswith(mod)]
    if contained:
        return True
    return False


def make_hashkey(seed):
    """ Generate a hashkey (string) """
    h = hashlib.md5()
    h.update(str(seed))
    return h.hexdigest()
