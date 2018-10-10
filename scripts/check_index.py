#!/usr/bin/env python

import argparse
import sys

from elasticsearch import Elasticsearch

# TODO - specify a minimum number of documents (implies checking one index at a time)

def main():

    parser = argparse.ArgumentParser(description='Elasticsearch index checker. Connect to a specified Elasticsearch '
                                                 'instance, check for the existence of one or more indices.\n'
                                                 'If an index is missing, print its name and exit with a return code '
                                                 'of 1. Otherwise, exit with code 0.')

    parser.add_argument("-e", "--elasticsearch", required=True, help="URL of Elasticsearch instance to check")

    parser.add_argument("-i", "--index", required=True, nargs="*", help="Names of one or more indices to check for")

    parser.add_argument("-z", "--zero-fail", action='store_true', default=False,
                        help="Fail if any index exists but has zero documents")

    parser.add_argument("-s", "--silent", action='store_true', default=False,
                        help="Don't print names of missing indices")

    parser.add_argument("-v", "--verbose", action='store_true', default=False,
                        help="Print names of indices which are present as well as those missing")

    args = parser.parse_args()

    es = Elasticsearch(args.elasticsearch)

    exit_code = 0

    for index in args.index:
        if es.indices.exists(index):
            if args.verbose:
                print "Required index %s exists " % index
            if args.zero_fail:
                if es.count(index).get("count") == 0:
                    exit_code = 1
                    if not args.silent:
                        print "Required index %s exists but has zero entries" % index
        else:
            exit_code = 1
            if not args.silent:
                print "Required index %s does not exist" % index

    return exit_code


if __name__ == '__main__':
    sys.exit(main())
