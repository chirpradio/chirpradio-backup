#!/usr/bin/env python
from datetime import datetime
import optparse
import os
import subprocess
import sys
from textwrap import dedent


def getenv(name, environ=os.environ):
    if name not in environ:
        raise EnvironmentError('environment varable %s could not be found'
                               % name)
    return environ[name]


def main(argv=sys.argv, environ=os.environ, max_tries=10):
    parser = optparse.OptionParser(usage=dedent("""
                %prog [options]

                Synchronize the local archive directory with a remote
                Amazon S3 bucket.

                Environment Variables:

                CHIRP_ARCHIVES_DIR
                    Local path to complete archives folder.
                    Must end in the word 'archives'

                CHIRP_S3_BUCKET
                    Name of the Amazon S3 bucket in which to synchronize
                    archives.  It will be accessed like
                    s3://$CHIRP_S3_BUCKET/archives

                This is a wrapper around `s3cmd sync`. Be sure to run
                s3cmd --configure once as the executing user so you can set
                the Amazon S3 access key and secret.
                """).lstrip())
    parser.add_option('--full', action='store_true',
                      help=('Re-calculate md5sums of all files. This will '
                            'cause a remote read on ALL files. Without this, '
                            'files will not be uploaded when they exist by '
                            'name.'))
    (options, args) = parser.parse_args(argv)

    s3cmd = which('s3cmd', environ=environ)
    local_dir = getenv('CHIRP_ARCHIVES_DIR', environ=environ)
    if not local_dir.endswith('archives'):
        parser.error('Expected CHIRP_ARCHIVES_DIR to end in '
                     '"archives", got: %s' % local_dir)
    s3_bucket = getenv('CHIRP_S3_BUCKET', environ=environ)
    args = [s3cmd, 'sync']
    if not options.full:
        args += ['--skip-existing']
    args += [local_dir, 's3://%s/archives' % s3_bucket]
    tries = 0
    giving_up = False
    returncode = None

    while not giving_up:
        print_("START %s" % datetime.now())
        print_('CMD: %s' % ' '.join(args))
        tries += 1
        returncode = subprocess.call(args)
        if returncode == 0:
            # Success
            break
        sys.stderr.write(dedent("""
                                ----------------------------------------------
                                Caught error (tries=%s). Retrying...
                                ----------------------------------------------
                                """ % tries))
        if tries >= max_tries:
            giving_up = True
    print_("FINISHED %s %s"
           % (returncode==0 and 'successfully' or 'with errors',
              datetime.now()))
    sys.exit(returncode)


def print_(stmt):
    sys.stdout.write("%s\n" % stmt)


def which(cmdname, environ=os.environ):
    cmd = None
    for p in environ['PATH'].split(':'):
        c = os.path.join(p, cmdname)
        if os.path.exists(c):
            cmd = c
            break
    if not cmd:
        raise EnvironmentError("Could not locate %s on $PATH" % cmdname)
    return cmd


if __name__ == '__main__':
    main()
