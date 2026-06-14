#!/bin/bash -l

#SBATCH --job-name=hma
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --time=0:30:00
#SBATCH --mem=8GB
#SBATCH --account=torch_pr_351_general
#SBATCH --output=/scratch/adl9602/tx/logs_out/%j.out
#SBATCH --error=/scratch/adl9602/tx/logs_err/%j.err

/scratch/adl9602/venvs/py311/bin/python /scratch/adl9602/tx/hma_permits.py --state TX --permits /scratch/adl9602/tx/data/tx/tx_flood_elevation.parquet --hma /scratch/adl9602/tx/data/HazardMitigationAssistanceProjects.csv --out /scratch/adl9602/tx/data/tx/tx_flood_elev_hma.parquet
