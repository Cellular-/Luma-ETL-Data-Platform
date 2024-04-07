from utilities.utilities import bc_config_summary
import sys

def pprint_dict(d):
    for k,v in d.items():
        if isinstance(v, dict):
            pprint_dict(v)
        else:
            print(
                f'{k}: {v}'
            )

def main(args):
    print('Configuration Summary for', sys.argv[1])
    pprint_dict(bc_config_summary(args))

if __name__ == '__main__':
    main(sys.argv[1])