#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=56
#SBATCH --partition=cpu2
##
#SBATCH --job-name=vds_processing
#SBATCH --output=vds_processing.%j.out
#SBATCH --error=vds_processing.%j.err
##
## This script runs vds_multiproessing.py using slurm

python vds_multiprocessing.py 20190101 20190131 holidays/holidays_2019.csv ex_data/201901 output