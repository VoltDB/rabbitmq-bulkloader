# RabbitMQ Bulk Loader


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


## Folder structure

### rabbitmq-bulk-loader

Contains source and object files for the RabbitMQ to VoltDB bulk loader.

### rabbitmq-utility

Contains source and object files for utility classes used by the
bulk loader and also by test programs.


### scripts

Contains convenience scripts to administer the project and to simplify
running the programs that are produced.



## Running

### Running with gradle

Programs can be run using gradle as follows:

```bash
gradle <program>:run [-Drun.args="<command-line-arguments>"]
```

This gets the help screen for the bulk loader:

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

### Running with scripts

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
