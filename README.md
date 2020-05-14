SqlShell - JDBC based SQL Shell
===============================

[![Build Status](https://api.travis-ci.com/realityforge/sqlshell.svg?branch=master)](http://travis-ci.org/realityforge/sqlshell)

What is SqlShell ?
------------------

SqlShell is a simple shell that accepts commands from standard input and executes
then using JDBC connection. The shell is used as the basis for automating databases.
It was created to simplify interaction with databases from Chef, regardless of the
host operating system and reasonable independent of the underlying database server.

The tool is used in production to automate Postgres and SQL Server databases from
Windows and Linux hosts.

The command shell is reasonably primitive and is designed to be driven by a
configuration management tool such as Chef. Commands are expected to be separated
by the text "GO" on a new line and must return a result set. The result set is emitted
to standard out  as json.
