#!/bin/bash
#SBATCH --job-name=heatwave_projections_py_mpi      # Job name
#SBATCH -p batch        # partition (this is the queue your job will be added to) 
#SBATCH --ntasks=24                  # Number of MPI ranks
#SBATCH --cpus-per-task=1            # Number of cores per MPI rank 
#SBATCH --nodes=4                    # Number of nodes
#SBATCH --ntasks-per-node=16         # How many tasks on each node
#SBATCH --ntasks-per-socket=8        # How many tasks on each CPU or socket
#SBATCH --distribution=cyclic:cyclic # Distribute tasks cyclically on nodes and sockets
#SBATCH --mem-per-cpu=2000mb          # Memory per processor
#SBATCH --time=36:00:00 # time allocation, which has the format (D-HH:MM), here set to 36 hours
#SBATCH --mail-type=ALL              # Mail events (NONE, BEGIN, END, FAIL, ALL)
#SBATCH --mail-user=jeffrey.newman@adelaide.edu.au  # Where to send mail
#SBATCH --output=mpi_test_%j.out     # Standard output and error log
pwd; hostname; date

echo "Running prime number generator program on $SLURM_JOB_NUM_NODES nodes with $SLURM_NTASKS tasks, each with $SLURM_CPUS_PER_TASK cores."

module load Python/3.6.0-foss-2016uofa
module load R/3.3.0-foss-2016uofa
module load GDAL/2.1.0-foss-2016uofa

mpirun -np 24 python "/home/a1091793/Code/HeatwaveAnalaysis/Heatrisk_South_Australia_GoyderProjections/ParallelHeatRiskAnalysis2Phoenix.py"