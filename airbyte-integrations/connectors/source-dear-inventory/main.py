#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_dear_inventory import SourceDearInventory

if __name__ == "__main__":
    source = SourceDearInventory()
    launch(source, sys.argv[1:])
