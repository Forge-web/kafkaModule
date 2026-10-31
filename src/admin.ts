import { Kafka } from 'kafkajs';

import { KAFKA_BROKERS, CLIENT_ID, TOPICS } from './config'

export async function createTopics() {
    // создание клента
    const kafka= new Kafka({
        clientId: CLIENT_ID,
        brokers: KAFKA_BROKERS
    });

    // получаем админа
    const admin = kafka.admin();

    // подключаемся
    await admin.connect();
    console.log('Админ подключен')

    // создаем топики
    try {
        await admin.createTopics({
            topics: [
                { topic: TOPICS.RAW, numPartitions: 1, replicationFactor: 1 },
                { topic: TOPICS.VALID, numPartitions: 1, replicationFactor: 1 },
                { topic: TOPICS.INVALID, numPartitions: 1, replicationFactor: 1 }
            ]
        });
        console.log('Топики созданы!');
    } catch (error) {
        console.log ('Ошибка:', error);
    } finally {
        // Отключаемся
        await admin.disconnect()
    }
}