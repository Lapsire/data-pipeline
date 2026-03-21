require('dotenv').config({ path: '../.env' }); // When local, not mandatory in docker

const SOCKET_EVENTS = {
    USER_UPDATE_EVENT: 'user_update',
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

const sendToKafka = async(data, topic) => {
    await producer.send({
        topic: topic,
        messages: [
            { value: JSON.stringify(data) }
        ]
    });
};

// ROUTES
app.post(
    '/user',
    async (req, res) => {
        const data = req.body;

       try {
            await sendToKafka(data, process.env.KAFKA_TOPIC);
            io.emit(SOCKET_EVENTS.USER_UPDATE_EVENT, data);

            return res.status(200).json({
                message: "OK"
            });
       } catch (error) {
            res.status(500).json({
                error: "Kafka error"
            });

            console.error("Kafka error :", error.message);
       } 
    }
);

// LAUNCH
const run = async() => {
    await producer.connect();
    console.log("Kafka connected.");

    server.listen(process.env.API_PORT || 3000, () => {
        console.log(`LAUNCH APP ON http://localhost:${process.env.API_PORT}`);
    });
}

run().catch(console.error);