GCS-SecretManager-SpringBoot POC

Step 1 clone 
Step 2 gcloud auth login inside intelliJ terminal and authenticate to a GCP project in which you have access to secret manager, GCS 
Step 3 in gcp create a bucket to which pdf files can be uploaded/downloaded 
Step 4 in gcp console, create a secret named secret-bucket with the name of the above bucket 
Step 5 mvn clean 
Step 6 mvn install 
Step 7 mvn package 
Step 8 Run the app 
Step 9 Hit end points using postman (Refer controller for help)
