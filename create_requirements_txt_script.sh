cd dividends
pipreqs --force .
sed -i '/^protobuf==[0-9.]\+/d' requirements.txt
pip freeze | grep google-cloud-bigquery >> requirements.txt
pip freeze | grep google-cloud-storage >> requirements.txt
pip freeze | grep google-cloud-logging >> requirements.txt
pip freeze | grep google-cloud-secret-manager >> requirements.txt