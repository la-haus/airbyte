#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_hubspot_history_deals import SourceHubspotHistoryDeals

if __name__ == "__main__":
    source = SourceHubspotHistoryDeals()
    launch(source, sys.argv[1:])
