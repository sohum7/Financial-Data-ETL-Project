# Go to the extract directory and generate requirements.txt using pipreqs
cd dividends
req_path="transform"
touch $req_path/temp_requirements.txt

pipreqs --force transform --print >> $req_path/temp_requirements.txt
pipreqs --force shared --print >> $req_path/temp_requirements.txt

# Remove any existing protobuf and shared dependencies from requirements.txt
sed -i '/^protobuf==[0-9.]\+/d' $req_path/temp_requirements.txt
sed -i '/^shared==[0-9.]\+/d' $req_path/temp_requirements.txt

# Add specific versions of google-cloud-storage, google-cloud-logging, and google-cloud-secret-manager to requirements.txt
pip freeze | grep google-cloud-storage >> $req_path/temp_requirements.txt
pip freeze | grep google-cloud-logging >> $req_path/temp_requirements.txt
pip freeze | grep google-cloud-secret-manager >> $req_path/temp_requirements.txt

sort $req_path/temp_requirements.txt | uniq > $req_path/requirements.txt

rm -f $req_path/temp_requirements.txt