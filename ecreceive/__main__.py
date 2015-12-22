import sys

import ecreceive.threads


def main():
    program = ecreceive.threads.MainThread()
    sys.exit(program.run())


if __name__ == '__main__':
    main()
