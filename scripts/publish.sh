#!/bin/bash

sbt packArchive
scp target/fluvius-p1-scala-1.0.tar.gz heli:runtimes/