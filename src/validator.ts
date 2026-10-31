import { Kafka } from 'kafkajs';
import { KAFKA_BROKERS, CLIENT_ID, TOPICS } from './config.js';
import { sendMessage } from './producer.js';

// 1. Функция проверки чека
function isValid(receipt: any): boolean {
    // Проверяем, что есть ID и позиции
    if (!receipt.id || !receipt.positions || receipt.positions.length === 0) {
        return false;
    }

    // Считаем сумму всех позиций
    const calculatedSum = receipt.positions.reduce((acc: number, pos: any) => {
        return acc + (pos.price * pos.quantity);
    }, 0);

    // Сверяем с check_sum (допускаем погрешность 0.01)
    const isSumCorrect = Math.abs(calculatedSum - receipt.check_sum) < 0.01;

    return isSumCorrect;
}

// 2. Функция запуска Валидатора
export async function startValidator() {
    const kafka = new Kafka({
        clientId: CLIENT_ID,
        brokers: KAFKA_BROKERS,
    });

    const consumer = kafka.consumer({ groupId: 'validator-group' });

    await consumer.connect();
    // Подписываемся на "сырой" топик
    await consumer.subscribe({ topic: TOPICS.RAW, fromBeginning: true });

    console.log(`👂 Валидатор слушает ${TOPICS.RAW}...`);

    await consumer.run({
        eachMessage: async ({ message }) => {
            if (!message.value) return;

            try {
                // Парсим JSON
                const receipt = JSON.parse(message.value.toString());

                // Проверяем
                const valid = isValid(receipt);
                const targetTopic = valid ? TOPICS.VALID : TOPICS.INVALID;
                const status = valid ? '✅ ВАЛИДЕН' : '❌ НЕВАЛИДЕН';

                console.log(`Проверка чека ${receipt.id}: ${status}`);

                // Пересылаем в нужный топик
                await sendMessage(targetTopic, receipt);

            } catch (error: any) {
                console.error('Ошибка обработки сообщения:', error.message);
                // Если JSON битый, тоже отправляем в invalid
                await sendMessage(TOPICS.INVALID, {
                    error: 'Invalid JSON',
                    raw: message.value.toString()
                });
            }
        },
    });
}