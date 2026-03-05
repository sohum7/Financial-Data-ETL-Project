# Go to the extract directory and generate requirements.txt using pipreqs
cd dividends/extract
pipreqs --force .

# Remove any existing protobuf and shared dependencies from requirements.txt
sed -i '/^protobuf==[0-9.]\+/d' requirements.txt
sed -i '/^shared==[0-9.]\+/d' requirements.txt

# Add specific versions of google-cloud-storage, google-cloud-logging, and google-cloud-secret-manager to requirements.txt
pip freeze | grep google-cloud-storage >> requirements.txt
pip freeze | grep google-cloud-logging >> requirements.txt
pip freeze | grep google-cloud-secret-manager >> requirements.txt

# Move final requirements.txt to jobs folder
mv requirements.txt jobs/requirements.txt