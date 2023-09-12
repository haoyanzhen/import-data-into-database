#!/bin/bash

nohup mpirun -np 24 python -u mpicsv2db.py >output.txt 2>&1 &
