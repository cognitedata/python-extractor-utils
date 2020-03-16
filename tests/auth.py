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
