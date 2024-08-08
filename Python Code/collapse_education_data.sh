#!/bin/bash

#SBATCH --job-name=educ_aggregate
#SBATCH --output=educ_aggregate%j.out
#SBATCH --error=educ_aggregate%j.err
#SBATCH --mem=250G           # Total memory limit
#SBATCH --mail-type=END,FAIL # Notify on job completion and failure
#SBATCH --mail-user=ayeddana@ucsd.edu # Your email address

# Print the start time
echo "Job started at $(date)"

# Activate your Python environment if you have one
# source /home/labs/bharadwajlab/ayeddana/ay_virtual_env/activate

# Navigate to the directory containing your Python script
echo "Navigating to the script directory"
cd /labs/bharadwajlab/ayeddana/
echo "Current directory: $(pwd)"

# Run your Python script
echo "Running Python script"
python collapse_education_data.py

# Print the end time
echo "Job finished at $(date)"