import { Kafka } from "kafkajs";
import { KAFKA_BROKERS, CLIENT_ID, TOPICS } from "./config.js";
import { sendMessage } from "./producer.js";
import { Decimal } from "decimal.js";
import { error } from "node:console";
import { ValidateResult } from "./types.js";

// 1. Функция проверки чека
export function validateReceipt(receipt: unknown): ValidateResult {
	const errors: string[] = [];
	const r = receipt as Record<string, unknown>;
	let items_sum = new Decimal(0);

	if (!r.id) {
		errors.push("Отсутствует `id` чека");
	}

	if (!r.created_at) {
		errors.push("Отсутствует `created_at` в чеке");
	}

	if (!Array.isArray(r.items)) {
		errors.push("Позиции в чеке должны быть в виде массива");
	} else if (r.items.length <= 0) {
		errors.push("Отсутсвуют позиции в чеке");
	} else {
		for (const item of r.items) {
			if (
				Decimal(item.sum).lte(0) ||
				Decimal(item.quantity).lte(0) ||
				Decimal(item.price).lte(0)
			) {
				errors.push(
					"Позиция в чеке не может иметь `sum`, `price` или `quantity` равное или меньше 0",
				);
				break;
			} else if (
				!Decimal(item.sum).eq(
					Decimal(item.price).mul(Decimal(item.quantity)),
				)
			) {
				errors.push(
					"`sum` позиции должна быть равна `price` * `quantity`",
				);
			} else {
				items_sum = items_sum.plus(item.sum);
			}
		}
	}

	if (
		typeof r.check_sum !== "string" &&
		Decimal(String(r.check_sum)).isInteger()
	) {
		errors.push(
			`В чеке не верно указана сумма ${typeof r.check_sum}`,
		);
	} else if (Decimal(String(r.check_sum)).lte(0)) {
		errors.push("Сумма не может быть отрицательной");
	} else if (!Decimal(String(r.check_sum)).eq(items_sum)) {
		errors.push(
			"Сумма чека должа быть равна сумме `sum` всех позиций",
		);
	}

	if (errors.length > 0) {
		return { valid: false, data: receipt, errors };
	}

	return { valid: true, data: receipt };
}

// 2. Функция запуска Валидатора
// export async function startValidator() {
// 	const kafka = new Kafka({
// 		clientId: CLIENT_ID,
// 		brokers: KAFKA_BROKERS,
// 	});

// 	const consumer = kafka.consumer({ groupId: "validator-group" });

// 	await consumer.connect();
// 	// Подписываемся на "сырой" топик
// 	await consumer.subscribe({
// 		topic: TOPICS.RAW,
// 		fromBeginning: true,
// 	});

// 	console.log(`👂 Валидатор слушает ${TOPICS.RAW}...`);

// 	await consumer.run({
// 		eachMessage: async ({ message }) => {
// 			if (!message.value) return;

// 			try {
// 				// Парсим JSON
// 				const receipt = JSON.parse(message.value.toString());

// 				// Проверяем
// 				const valid = isValid(receipt);
// 				const targetTopic = valid ? TOPICS.VALID : TOPICS.INVALID;
// 				const status = valid ? "✅ ВАЛИДЕН" : "❌ НЕВАЛИДЕН";

// 				console.log(`Проверка чека ${receipt.id}: ${status}`);

// 				// Пересылаем в нужный топик
// 				await sendMessage(targetTopic, receipt);
// 			} catch (error: any) {
// 				console.error("Ошибка обработки сообщения:", error.message);
// 				// Если JSON битый, тоже отправляем в invalid
// 				await sendMessage(TOPICS.INVALID, {
// 					error: "Invalid JSON",
// 					raw: message.value.toString(),
// 				});
// 			}
// 		},
// 	});
// }
