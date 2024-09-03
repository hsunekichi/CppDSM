#!/bin/bash

cd ..
rsync -av --exclude='.git' --exclude='.gitignore' rtdb-redis a816678@central.cps.unizar.es:/tmp/a816678/
cd rtdb-redis