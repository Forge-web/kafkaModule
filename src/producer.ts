import { Kafka } from "kafkajs";

import { KAFKA_BROKERS, CLIENT_ID } from "./config";

export async function sendMessage(topic: string, message: any) {
    // создание клента
    const kafka= new Kafka({
        clientId: CLIENT_ID,
        brokers: KAFKA_BROKERS
    });

    // создание продюсера
    const producer = kafka.producer();

    try {
        
        await producer.connect();
        
        await producer.send({
            topic: topic,
            messages: [{ value: JSON.stringify(message) }]
        });

        console.log(`Отправлено в ${topic}`);
    } catch (error) {
        console.error(`Ошибка отправки в ${topic}:`, error);
    } finally {
        await producer.disconnect();
    }
}