export EUREKA_URI=3.136.74.174:8090
export EUREKA_PASS=W872INBJNWZcoDN
export EUREKA_USER=someUserName

export COCKROACH_USER="monitoring"
export COCKROACH_PASS="9gLufC2BVxta3O6m"
export COCKROACH_URI="postgresql://onedatashare-627.j77.cockroachlabs.cloud:26257/onedatashare?sslmode=prefer"

export INFLUX_TOKEN="CkCFcu0sca6udDVk3A0Bbj1rfwilGSEAp8A36Qy7LOhE2DhnVqAtoT1QpD9dKWwVIr4oIczft0ypWhRNNKN0XA=="
export INFLUX_ORG="10972f62ad2d59a6"

mvn clean package -DskipTests

java -Dspring.profiles.active=dev -jar target/ods-metadata-0.0.1-SNAPSHOT.jar
