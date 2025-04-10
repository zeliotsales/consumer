require('dotenv').config();
const { Kafka } = require('kafkajs');

const inputTopic = process.env.KAFKA_INPUT_TOPIC;

const kafka = new Kafka({
    clientId: 'json-consumer',
    brokers: ["my-cluster-kafka-bootstrap.kafka:9092"], // KAFKA BROKER
    sasl: {
        mechanism: "scram-sha-512",
        username: "my0jpefb0c6q7u8fkfeqjb0t7", // Use environment variables for credentials
        password: "a8438SL6T2Vx4FJTuntCtzyTV5vl56JO",
    },
});

const consumer = kafka.consumer({ groupId: `json-consumer-group-${Date.now()}` });

const consumeMessages = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: inputTopic, fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                const value = message.value.toString();
                console.log(`Received message:`, value);
            } catch (error) {
                console.error('Error processing message:', error);
                console.error('Message content:', message.value ? message.value.toString() : 'undefined');
            }
        }
    });
};

consumeMessages().catch(console.error);
