docker compose down -v --remove-orphans
docker compose build --no-cache
docker compose up --scale grpc-server=3 -d