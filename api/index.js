require('dotenv').config({ path: '../.env' }); // When local, not mandatory in docker

const SOCKET_EVENTS = {
    USER_UPDATE_EVENT: 'user_update',
    METRICS_UPDATE_EVENT: 'metrics_update'
};

// IMPORTS
const express = require('express');
const http = require('http');
const { Kafka } = require('kafkajs');
const { Server } = require('socket.io');

const app = express();                
const server = http.createServer(app);
const io = new Server(server);        

app.use(express.json());
app.use(express.static('public')); // SERVE DASHBOARD

// PRODUCER KAFKA
const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID,
  brokers: [process.env.KAFKA_BROKER] // KAFKA address
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'api-metrics-group' });

const sendToKafka = async(data, topic) => {
    await producer.send({
        topic: topic,
        messages: [
            { value: JSON.stringify(data) }
        ]
    });
};

const startConsumer = async() => {
    await consumer.connect();
    await consumer.subscribe({ topic: process.env.KAFKA_TOPIC_METRICS || 'metrics' });
    await consumer.run({
        eachMessage: async ({ message }) => {
            const data = message.value.toString(); // Kafka messages comes in bytes
            const parsedData = JSON.parse(data);
            io.emit(SOCKET_EVENTS.METRICS_UPDATE_EVENT, parsedData);
        }
    });
};

app.post(
    '/users/batch',
    async (req, res) => {
        const batch = req.body;

        if (!Array.isArray(batch)) {
            return res.status(400).json({ error: "Data must be an array." });
        }

        try {
            const messages = batch.map(data => ({ value: JSON.stringify(data) }));

            await producer.send({
                topic: process.env.KAFKA_TOPIC_DATA,
                messages: messages
            });

            io.emit('user_update_batch', batch);

            return res.status(200).json({
                message: "Batch OK",
                count: batch.length
            });
        } catch (error) {
            console.error("Kafka error :", error.message);
            res.status(500).json({ error: "Kafka error" });
        }
    }
);

// LAUNCH
const run = async() => {
    await producer.connect();
    console.log("Kafka connected.");
    await startConsumer();
    console.log("Consume started.");

    server.listen(process.env.API_PORT || 3000, () => {
        console.log(`LAUNCH APP ON http://localhost:${process.env.API_PORT}`);
    });
}

run().catch(console.error);