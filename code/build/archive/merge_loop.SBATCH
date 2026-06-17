#!/bin/bash -l

#SBATCH --job-name=merge
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --time=8:00:00
#SBATCH --mem=128GB
#SBATCH --account=torch_pr_351_general
#SBATCH --output=/scratch/adl9602/tx/logs_out/%j.out
#SBATCH --error=/scratch/adl9602/tx/logs_err/%j.err

mkdir -p /scratch/adl9602/tmp
mkdir -p /scratch/adl9602/tx/logs_out
mkdir -p /scratch/adl9602/tx/logs_err

/scratch/adl9602/venvs/py311/bin/python /scratch/adl9602/tx/merge_loop.py