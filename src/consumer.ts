import { Consumer } from "kafkajs";
import { kafka } from "./kafka.js";
import { validateReceipt } from "./validator.js";
import { sendMessage } from "./producer.js";
import { DeadLetterMessage } from "./types.js";
import { TOPICS } from "./config.js";

const consumer: Consumer = kafka.consumer({
	groupId: "validator-group",
});

export async function startConsumer(): Promise<void> {
	await consumer.connect();
	await consumer.subscribe({
		topic: TOPICS.RAW,
		fromBeginning: false,
	});

	await consumer.run({
		eachMessage: async ({ partition, message }) => {
			const raw = message.value?.toString();
			const key = message.key?.toString();

			let parsed: unknown;

			try {
				parsed = JSON.parse(raw ?? "");
			} catch {
				console.warn(
					`⚠️ Невалидный JSON в партиции ${partition}:`,
					raw,
				);
				const dlqMessage: DeadLetterMessage = {
					originalMessage: raw,
					errors: ["Invalid JSON"],
				};
				return;
			}
			const result = validateReceipt(parsed);

			if (!result.valid) {
				const dlqMessage: DeadLetterMessage = {
					originalMessage: parsed,
					errors: result.errors,
				};
				console.log(
					`\nЧек не валиден\n${JSON.stringify(result.errors)}\n`,
				);
				await sendMessage(TOPICS.INVALID, dlqMessage, key);
				return;
			}
			console.log(`\nЧек валиден\n`);
			await sendMessage(TOPICS.VALID, result.data, key);
		},
	});
}

export async function stopConsumer(): Promise<void> {
	await consumer.disconnect();
}
