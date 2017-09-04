Use S3 as an HSM for dcache
======================================

Usage
-----
```
 s3hsm - use S3 and HSM for dCache

Usage:
    $ s3hsm put <pnfsid> <path> [-key[=value]...]
    $ s3hsm get <pnfsid> <path> -uri=<uri>[-key[=value]...]
    $ s3hsm remove -uri=<uri> [-key[=value]...]

Options:
    -debuglog=<filename>    : log debug informaion into specified file.
    -s3config=<filename>    : path to is3 endpoint config file.
    -s3endpoint=<host:port> : S3 endpoint's host and port
    -s3usessl               : use https protocol when talking to S3 endpoint.
    -s3bucket=<bucket>      : name of S3 bucket to use.
    -s3key=<key>            : S3 AccessKey, overwrites the value from config file.
    -s3secret=<secret>      : S3 SecretAccessKey, overwrites the value from config file.
    -enc                    : Encrypt data with a random key before sending to S3 storage.
```

Flush file to s3
-----------------
```
$ s3hsm put 0000000635D5968A4DD89E29C242185B2D82 /dcache/pool1/0000000635D5968A4DD89E29C242185B2D82 \
    -s3bucket=data \
    -s3key=ACCESS_KEY \
    -s3secret=SECRET_KEY \
    -s3endpoint=127.0.0.1:9000
```
The script will return back location uri:
```
s3://s3/data/0000000635D5968A4DD89E29C242185B2D82
```

Restore file from s3
--------------------
```
$ s3hsm get 0000000635D5968A4DD89E29C242185B2D82 /dcache/pool1/0000000635D5968A4DD89E29C242185B2D82 \
    -uri=s3://data/0000000635D5968A4DD89E29C242185B2D82 \
    -s3bucket=data \
    -s3key=ACCESS_KEY \
    -s3secret=SECRET_KEY \
    -s3endpoint=127.0.0.1:9000
```

Remove file from s3
-------------------
````
$ s3hsm remove -uri=s3://data/0000000635D5968A4DD89E29C242185B2D82 \
    -s3bucket=data \
    -s3key=ACCESS_KEY \
    -s3secret=SECRET_KEY \
    -s3endpoint=127.0.0.1:9000
````

To simplify command line usage, a config file with connection properties can be provides:
```
$ s3hsm put 0000000635D5968A4DD89E29C242185B2D82 /dcache/pool1/0000000635D5968A4DD89E29C242185B2D82 \
    -s3bucket=data \
    -s3config=/path/to/config.yml
```
The config file as a very simple format:
```yaml
s3:
  endpoint: 127.0.0.1:9000
  access_key: ACCESS_KEY
  secret_key: SECRET_KEY
  ssl: false
  enc: false
  trace: false
  s3version: 4

hsm:
  instance: amazon-s3
  type: osm
```

Building from sources
=====================

```
$ go get -u github.com/aws/aws-sdk-go/aws
$ go build
```

License:
--------

licensed under [GPLv3](http://www.gnu.org/licenses/gpl-3.0.txt "GPLv3") (or later)

How to contribute
=================

**s3hsm** uses the linux kernel model where git is not only source repository,
but also the way to track contributions and copyrights.

Each submitted patch must have a "Signed-off-by" line.  Patches without
this line will not be accepted.

The sign-off is a simple line at the end of the explanation for the
patch, which certifies that you wrote it or otherwise have the right to
pass it on as an open-source patch.  The rules are pretty simple: if you
can certify the below:
```

    Developer's Certificate of Origin 1.1

    By making a contribution to this project, I certify that:

    (a) The contribution was created in whole or in part by me and I
         have the right to submit it under the open source license
         indicated in the file; or

    (b) The contribution is based upon previous work that, to the best
        of my knowledge, is covered under an appropriate open source
        license and I have the right under that license to submit that
        work with modifications, whether created in whole or in part
        by me, under the same open source license (unless I am
        permitted to submit under a different license), as indicated
        in the file; or

    (c) The contribution was provided directly to me by some other
        person who certified (a), (b) or (c) and I have not modified
        it.

    (d) I understand and agree that this project and the contribution
        are public and that a record of the contribution (including all
        personal information I submit with it, including my sign-off) is
        maintained indefinitely and may be redistributed consistent with
        this project or the open source license(s) involved.

```
then you just add a line saying ( git commit -s )

    Signed-off-by: Random J Developer <random@developer.example.org>

using your real name (sorry, no pseudonyms or anonymous contributions.)
