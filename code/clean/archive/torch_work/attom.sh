#!/bin/bash -l

#SBATCH --job-name=attom
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --time=1:00:00
#SBATCH --mem=28GB
#SBATCH --account=torch_pr_351_general
#SBATCH --output=/scratch/adl9602/tx/logs_out/%j.out
#SBATCH --error=/scratch/adl9602/tx/logs_err/%j.err

/scratch/adl9602/venvs/py311/bin/python /scratch/adl9602/tx/attom_onto_permits.py --state TX --permits /scratch/adl9602/tx/data/tx/tx_flood_elev_hma.parquet --attom /scratch/adl9602/tx/data/tx/attom_tx.parquet --out /scratch/adl9602/tx/data/tx/tx_attom_permits.parquet --tmp /scratch/adl9602/tx/tmp --threads 4 --memory 24GB
