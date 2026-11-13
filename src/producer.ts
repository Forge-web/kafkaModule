import { Producer } from "kafkajs";
import { kafka } from "./kafka.js";

const producer: Producer = kafka.producer();

let isConnected = false;

export async function connectProducer(): Promise<void> {
	if (!isConnected) {
		await producer.connect();
		isConnected = true;
	}
}

export async function sendMessage<T>(
	topic: string,
	value: T,
	key?: string | null,
): Promise<void> {
	await connectProducer();
	await producer.send({
		topic,
		messages: [{ key: key ?? null, value: JSON.stringify(value) }],
	});
}

export async function disconnectProducer(): Promise<void> {
	if (isConnected) {
		await producer.disconnect();
		isConnected = false;
	}
}
