import os

# Folder path where the markdown files are located
folder_path = 'output_markdown'

# Iterate over all the files in the folder
for filename in os.listdir(folder_path):
    # Check if the file has a .md extension
    if filename.endswith(".md"):
        # Define the new filename with the prefix
        new_filename = f"תקנון הכנסת_{filename}"
        
        # Construct the full file paths
        old_file = os.path.join(folder_path, filename)
        new_file = os.path.join(folder_path, new_filename)
        
        # Rename the file
        os.rename(old_file, new_file)
        
        # Print the renaming action
        print(f'Renamed: {filename} --> {new_filename}')

print("All files renamed successfully.")
