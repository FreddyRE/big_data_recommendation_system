#KAFKA CONNECT

curl -s -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @s3-sink-clickstreamjson
curl -s -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @s3-sink-product-features.json
curl -s -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @s3-sink-user-features.json
curl -s -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @s3-sink-recommendation-signals.json
