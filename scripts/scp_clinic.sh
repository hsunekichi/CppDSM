#!/bin/bash

cd ..
rsync -av --exclude='.git' --exclude='.gitignore' rtdb-redis a816678@155.210.150.96:/tmp/a816678/
cd rtdb-redis