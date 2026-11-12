import { Admin } from "kafkajs";
import { kafka } from "./kafka";

import { KAFKA_BROKERS, CLIENT_ID, TOPICS } from "./config";

const admin: Admin = kafka.admin();

export async function createTopics() {
	await admin.connect();

	try {
		const metadata = await admin.fetchTopicMetadata();
		const topics = metadata.topics.map((t) => t.name);

		if (topics.length === 0) {
			await admin.createTopics({
				topics: [
					{
						topic: TOPICS.RAW,
						numPartitions: 1,
						replicationFactor: 1,
					},
					{
						topic: TOPICS.VALID,
						numPartitions: 1,
						replicationFactor: 1,
					},
					{
						topic: TOPICS.INVALID,
						numPartitions: 1,
						replicationFactor: 1,
					},
				],
			});
			console.log("Топики созданы!");
		} else {
			console.log("Топики уже были созданы");
		}
	} catch (error) {
		console.log("Ошибка:", error);
	} finally {
		// Отключаемся
		await admin.disconnect();
	}
}
