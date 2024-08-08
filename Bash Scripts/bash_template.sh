#!/bin/bash

#SBATCH --job-name=<job_name>           # SET JOB NAME
#SBATCH --output=<job_name>%j.out       # SET JOB NAME
#SBATCH --error=<job_name>%j.err        # SET JOB NAME
#SBATCH --mem=250G                      # Total memory limit
#SBATCH --mail-type=END,FAIL            # Notify on job completion and failure
#SBATCH --mail-user=<username>@ucsd.edu # SET EMAIL ADRESS

# Print the start time
echo "Job started at $(date)"

# Activate your Python environment if you have one - this is optional for the SSRDE SLURM Cluster given its configuration
# source /home/labs/bharadwajlab/ayeddana/ay_virtual_env/activate

# Navigate to the directory containing your Python script
echo "Navigating to the script directory"

cd /labs/bharadwajlab/ayeddana/ # ALTER THIS AS NEEDED

echo "Current directory: $(pwd)"

# Run your Python script
echo "Running Python script"
python <python_file>          # SET THE NAME OF PYTHON SCRIPT TO RUN

# Print the end time
echo "Job finished at $(date)"
