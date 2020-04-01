#  Copyright 2020 Cognite AS
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging

_logger = logging.getLogger(__name__)

"""
Integration test against Azure Active Directory (AAD). Requires a test environment where an AAD tenant is 
associated with the CDF project, and the security group in CDF linked to a group in AAD.

We don't have such an infrastructure established for a Jenkins builds, so leaving it for an ad-hoc test for now.
"""


def main():
    logging.basicConfig(level=logging.DEBUG)
    from cognite.extractorutils.configtools import load_yaml, CogniteConfig

    config = load_yaml(open("config.yaml"), CogniteConfig)
    cdf = config.get_cognite_client("AAD test")
    print("Login status", cdf.login.status())

    tss = cdf.time_series.list(limit=100)
    print("Found %d time series" % len(tss))
    for ts in tss[:10]:
        print("  ", ts.name)


if __name__ == "__main__":
    main()
