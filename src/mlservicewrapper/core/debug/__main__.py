import argparse
import logging
import os

from .. import server
from . import local


#https://stackoverflow.com/a/42355279/1270504
class _StoreDictKeyPair(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        my_dict = {}
        for kv in values.split(","):
            k,v = kv.split("=")
            my_dict[k] = v
        setattr(namespace, self.dest, my_dict)

parser = argparse.ArgumentParser(description='Locally debug.', prog = "mlservicewrapper.core.debug")
parser.add_argument(
    '--config', help='Path to service configuration file', required=True)

parser.add_argument('--input-dir', dest='input_dir', help='Path to input directory')
parser.add_argument('--input-paths', dest='input_paths', help='Map input datasets to paths', action=_StoreDictKeyPair, metavar="<dataset name 1>=<path>,<dataset name 2>=<path>...")

parser.add_argument('--output-dir', dest='output_dir', help='Path to input directory')
parser.add_argument('--output-paths', dest='output_paths', help='Map output datasets to paths', action=_StoreDictKeyPair, metavar="<dataset name 1>=<path>,<dataset name 2>=<path>...")

parser.add_argument("--load-params", dest="load_params", action=_StoreDictKeyPair, metavar="KEY1=VAL1,KEY2=VAL2...")
parser.add_argument("--run-params", dest="runtime_parameters", action=_StoreDictKeyPair, metavar="KEY1=VAL1,KEY2=VAL2...")

parser.add_argument("--log-level")

parser.add_argument('--split-dataset-for-perf', dest='split_dataset_name',
                    help='Input dataset to split for performance evaluation.')

parser.add_argument('--assess-accuracy', dest='assess_accuracy', action=_StoreDictKeyPair, metavar="INPUT_DATASET_1.FIELD=OUTPUT_DATASET_1.FIELD,INPUT_DATASET_2.FIELD=OUTPUT_DATASET_2.FIELD,...",
                    help='Perform accuracy assessment against the given fields.')

parser.add_argument('--profile-processing-to-file', dest='profile_processing_to_file',
                    help='Enables profiling for the process phase, and writes results to a file which can then be read with pstats.')


args = parser.parse_args()

if args.log_level is not None:
    logging.basicConfig(level=args.log_level)

instance = server.ServerInstance(args.config)

local.run(
    instance,

    load_context=instance.build_load_context_source(override=args.load_params),

    runtime_parameters=args.runtime_parameters,

    input_dataset_paths=args.input_paths,
    input_dataset_directory=args.input_dir,

    split_dataset_name=args.split_dataset_name,
    
    output_dataset_directory=args.output_dir,
    output_dataset_paths=args.output_paths,
    
    assess_accuracy=args.assess_accuracy,

    profile_processing_to_file=args.profile_processing_to_file
)

instance.dispose()
