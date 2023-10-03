#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=32
#SBATCH --partition=cpu2
#SBATCH --job-name=vds_processing
#SBATCH --output="slurm/vds_processing.%j.out"
#SBATCH --error="slurm/vds_processing.%j.err"

# This script runs vds_multiproessing.py using slurm
python -u vds_multiprocessing.py 20201201 20201231 holidays/holidays_2020.csv ex_data/202012 output/202012
