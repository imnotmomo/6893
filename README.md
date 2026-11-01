# EECS 6893 Final Project

**AI Technology Radar**

We are building a data spine that combines arXiv, GitHub, and PyPI signals to forecast AI-tool adoption and distinguish sustained growth from short-term attention.

## Cluster setup

Our Spark cluster runs on three Oracle Cloud VMs. **c0** (2 OCPUs, 12 GB RAM) hosts the master, driver, and main worker. **c1** and **c2** (1 GB RAM each) are small workers used for distributed demos; normal jobs run on c0.

See the [connection guide](connection_guide.md) for cluster access and job submission.
