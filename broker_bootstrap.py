import argparse

import scylla


def _main(name=None):
    name = name or 'Broker'

    print '[BROKER-{0}] starting'.format(name)
    b = scylla.Broker(name)
    b.start()

    print '[BROKER-{0}] online'.format(name)
    b.join()

    print '[BROKER-{0}] offline'.format(name)

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-n', '--name', help='The name for the Broker')

    arg_parser.parse_args()

    _main(arg_parser.name)