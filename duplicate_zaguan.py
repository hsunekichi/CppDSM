



# Copy all files in zaguan to the same directory adding a prefix to the name, to duplicate the files
# This is useful to create a new set of files for testing purposes

import os
import shutil

# Get the current working directory
dup_dir = "./zaguan_medium"

# Get the list of files in the current working directory
files = os.listdir(dup_dir)


total_files = len(files)
i_file = 0

copy_number = 0

for file in files:
    # Get the highest copy number
    if file.startswith("c"):
        copy = file[1:file.find("_")]
        if int(copy) > copy_number:
            copy_number = int(copy)
    else:
        copy_number = 0
        
    # Print progress bar
    print(f"\rFinding copy number... {i_file}/{total_files}", end="")
    i_file += 1

i_file = 0
        

# Copy dup_dir to tmp_dup_dir
tmp_dup_dir = "./tmp_dup"
print(f"\nCopying {dup_dir} to {tmp_dup_dir}...")
shutil.copytree(dup_dir, tmp_dup_dir)

# Change name of files in tmp_dup_dir to add copy_number
files = os.listdir(tmp_dup_dir)
for file in files:
    os.rename(os.path.join(tmp_dup_dir, file), os.path.join(tmp_dup_dir, "c"+str(copy_number+1)+"_"+file))
    shutil.copy(os.path.join(tmp_dup_dir, "c"+str(copy_number+1)+"_"+file), os.path.join(dup_dir, "c"+str(copy_number+1)+"_"+file))
    
    # Print progress bar
    print(f"\rCopying file names... {i_file}/{total_files}", end="")
    i_file += 1
    
i_file = 0
    
print("Deleting tmp_dup_dir...")

# Remove tmp_dup_dir
shutil.rmtree(tmp_dup_dir)

print(f"\n{total_files} files copied to {dup_dir} with prefix c{copy_number+1}_")



"""
# Loop through the list of files
for file in files:
    # Get the full path of the file
    src = os.path.join(dup_dir, file)
    # Get the new name of the file
    dst = os.path.join(dup_dir, "c"+copy_number+"_" + file)

    # If the file is a directory, skip it
    if os.path.isdir(src):
        continue
    
    # Print progress bar
    print(f"\r{i_file}/{total_files}", end="")
    i_file += 1
    
    # Copy the file
    shutil.copy(src, dst)
    # Print the name of the file
    # print(dst)
"""


