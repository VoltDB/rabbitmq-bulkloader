# RabbitMQ Bulk Loader


## Overview

The VoltDB RabbitMQ bulk loader can populate VoltDB tables by using a
RabbitMQ message queue as a data source. It can consume messages that
are in comma-separated-value (CSV) format.

To run you will need a distribution archive file that contains all the
required dependencies, e.g. library jar files. The distribution also
provides a front end script to run the program. You can either build
the distribution yourself (see Building below) or download it
(t.b.d.).


## Installing the distribution

You may unpack the distribution archive any where you wish. It creates
a sub- directory with a name resembling the archive name. Feel free to
use either a graphical or a command line extraction tool. Use one of
the following shell commands to extract either the ".tgz" ("tarball")
or ".zip" archive files.

```bash
tar xvz rabbitmqloader-VERSION.tgz
unzip rabbitmqloader-VERSION.zip
```

Note that in a build environment the distribution files may be found
(after running "gradle assemble") in the following sub-directory:

```rabbitmq-bulk-loader/build/distributions```


## Running from the distribution

The working directory does not matter. The examples below are based on
the extracted archive root as the working directory. They also assume
that RabbitMQ is hosted on RHOST and VoltDB is hosted on VHOST.

Note that a variety of options are available to support different ways
of specifying RabbitMQ resources, e.g. with an exchange, a virtual
host, or an AMQP URI.

Refer to the command line help screen for the most up-to-date syntax
reference.

### Displaying command line help

```bin/rabbitmqloader --help```

### Example: Populate the VORDERS table from the RORDERS queue

```bin/rabbitmqloader --host RHOST --queue RORDERS --servers VHOST VORDERS```

### Example: Populate from the RORDERS queue via the AddOrder stored procedure

```bin/rabbitmqloader --host RHOST --queue RORDERS --servers VHOST -p AddOrder```


## Building

Here are a few useful build commands. Make sure Gradle (http://gradle.org)
is installed.

### List gradle tasks

```gradle tasks --all```

Retrieves a list of all possible gradle tasks (similar to ant targets).

### Build everything

```gradle build```

### Clean build output

```gradle clean```

### Build distribution archives

```gradle assemble```

### Generate Eclipse projects

```gradle eclipse```

### Clean Eclipse projects

```gradle cleaneclipse```


## Directory structure

### rabbitmq-bulk-loader

Contains source and object files for the RabbitMQ to VoltDB bulk loader.

### rabbitmq-utility

Contains source and object files for utility classes used by the
bulk loader and also by test programs.


### scripts

Contains convenience scripts to administer the project and to simplify
running the programs that are produced.



## Running in a build environment using Gradle

Programs can be run using gradle as follows:

```bash
gradle <program>:run [-Drun.args="<command-line-arguments>"]
```

This displays the help screen for the bulk loader:

```bash
gradle rabbitmq-bulk-loader:run -Drun.args=--help
```

This runs the send test and sends a 2 column CSV (string,integer) row
to the RabbitMQ queue named "test" every .1 to .5 seconds:

```bash
gradle test-rabbitmq-csv-send:run -Drun.args="--queue test -g si --sleepmin 100 --sleepmax 500"
```

This starts VoltDB, creates a corresponding 2 column table and runs the
bulk loader to load the above CSV output into that table.

```bash
voltdb create
sqlcmd --query="create table mytable (c1 varchar(100), c2 integer)"
gradle rabbitmq-bulk-loader:run -Drun.args="--queue test mytable"
```

## Running in a build environment using scripts

The scripts for running the built programs merely simplify the syntax
for invoking the appropriate gradle run task.

The above examples can also be run as follows. Note that you may want
to run the send command and or VoltDB in the background, e.g. by
appending '&' in bash or by running one or both in a separate shell.

```bash
scripts/rabbitmqloader --help
scripts/test-rabbitmq-csv-send --queue test -g si --sleepmin 100 --sleepmax 500
voltdb create
sqlcmd --query="create table mytable (c1 varchar(100), c2 integer)"
scripts/rabbitmqloader --queue test mytable
```


## TO-DO

- Move some utility classes to VoltDB client?
- Upload to Maven repository
- Print stats periodically (by insert count?) and before exiting, e.g. due to Ctrl-C.
- Add log4j.xml.
