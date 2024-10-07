Install & Run: https://typedb.com/docs/home/install

Download from TypeDB Package Repository: 

Server only: [Distributions for 2.29.1](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name:^typedb-server+version:2.29.1)

Server + Console: [Distributions for 2.29.1](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name:^typedb-all+version:2.29.1)


## New Features
- **Update docker image to have both x86_64 and arm64 versions**
Publish arm64 docker images. Previously, docker would fall-back to running the x86_64 images, which sometimes resulted in a JVM crash loop.

