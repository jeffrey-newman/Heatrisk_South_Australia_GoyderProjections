#!/bin/bash
#SBATCH --job-name=heatwave_projections_py_mpi      # Job name
#SBATCH -p batch        # partition (this is the queue your job will be added to) 
#SBATCH --ntasks=4                  # Number of MPI ranks
#SBATCH --nodes=2                    # Number of nodes
#SBATCH --mem-per-cpu=3000mb          # Memory per processor
#SBATCH --time=36:00:00 # time allocation, which has the format (D-HH:MM), here set to 36 hours
#SBATCH --mail-type=ALL              # Mail events (NONE, BEGIN, END, FAIL, ALL)
#SBATCH --mail-user=jeffrey.newman@adelaide.edu.au  # Where to send mail

pwd; hostname; date

echo "Running Heatwave analysis program (v Small) on $SLURM_JOB_NUM_NODES nodes with $SLURM_NTASKS tasks, each with $SLURM_CPUS_PER_TASK cores."

module load GEOS/3.5.0-foss-2016uofa
module load GDAL/2.1.0-foss-2016uofa
module load Python/3.6.0-foss-2016uofa
module load R/3.3.0-foss-2016uofa



mpirun -np 4 python "/home/a1091793/Code/HeatwaveAnalaysis/Heatrisk_South_Australia_GoyderProjections/ParallelHeatRiskAnalysis2Phoenix.py"