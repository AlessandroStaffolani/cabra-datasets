echo "Downloading trip data"

echo "Washington TripData"
wget https://s3.amazonaws.com/capitalbikeshare-data/202209-capitalbikeshare-tripdata.zip

echo "New York TripData"
wget https://s3.amazonaws.com/tripdata/202209-citibike-tripdata.csv.zip

echo "Bay Area TripData"
wget https://s3.amazonaws.com/baywheels-data/202209-baywheels-tripdata.csv.zip

echo "Boston TripData"
wget https://s3.amazonaws.com/hubway-data/202209-bluebikes-tripdata.zip

echo "All downloaded"
