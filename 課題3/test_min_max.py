from kafka.admin import KafkaAdminClient

admin_client = KafkaAdminClient(
    bootstrap_servers="150.65.230.59:9092",
    client_id='student-checker'
)

topics = admin_client.list_topics()
for t in topics:
    if t.startswith("i483-sensors-s2410040-"):
        print(t)
