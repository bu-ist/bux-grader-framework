import hashlib
import logging
import time

log = logging.getLogger(__name__)


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


def safe_multi_call(func, args, max_attempts=5, delay=5):
    result = None
    success = True
    attempt = 1
    while attempt <= max_attempts:
        try:
            result = func(*args)
            success = True
            break
        except Exception as e:
            log.warning("Safe multi call failed: %s (attempt %d of %d): %s",
                        func.__name__, attempt, max_attempts, e)

        # Sleep and try again
        time.sleep(delay)
        attempt += 1
    else:
        log.error("Safe multi call did not succeed after %d attempts: %s Args: %s",
                  max_attempts, func.__name__, args)
        success = False

    return result, success