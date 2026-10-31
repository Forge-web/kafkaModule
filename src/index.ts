import { createTopics } from './admin.js';
import { startValidator } from './validator.js';
import { sendMessage } from './producer.js';
import { TOPICS } from './config.js';

async function main() {
    // 1. Создаем топики (если их нет)
    await createTopics();

    // 2. Запускаем Валидатор (он начнет слушать raw_receipts в фоне)
    // Важно: не ставим await, чтобы он работал параллельно
    startValidator().catch(console.error);

    // 3. Отправляем тестовые чеки в raw_receipts
    // Сделаем паузу 2 секунды, чтобы Валидатор успел подписаться
    setTimeout(async () => {
        console.log('🚀 Отправляем тестовые чеки...');

        // Валидный чек
        await sendMessage(TOPICS.RAW, {
            "id": "uuid-чека",
            "users_id": "uuid-кассира",
            "organizations_id": 1,
            "payment_types": 1,
            "check_sum": 200.00,
            "created_at": "2026-10-31T12:00:00Z",
            "positions": [
                {
                    "product_items_id": "uuid-партии",
                    "product_id": 1,
                    "name": "Шина Michelin",
                    "quantity": 2.0,
                    "price": 100.00,
                    "sum": 200.00
                }
            ]
        });

        // Невалидный чек (сумма не сходится)
        await sendMessage(TOPICS.RAW, {
            "id": "uuid-чека-2",
            "users_id": "uuid-кассира-2",
            "organizations_id": 1,
            "payment_types": 1,
            "check_sum": 999.00, // <-- Неправильная сумма (должно быть 200)
            "created_at": "2026-10-31T12:05:00Z",
            "positions": [
                {
                    "product_items_id": "uuid-партии-2",
                    "product_id": 1,
                    "name": "Шина Michelin",
                    "quantity": 2.0,
                    "price": 100.00,
                    "sum": 200.00
                }
            ]
        });
    }, 2000);
}

main().catch(err => {
    console.error('❌ Ошибка:', err.message);
    process.exit(1);
});