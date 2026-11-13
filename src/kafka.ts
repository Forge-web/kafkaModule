import { Kafka, logLevel } from "kafkajs";
import { CLIENT_ID, KAFKA_BROKERS } from "./config.js";

export const kafka = new Kafka({
	clientId: CLIENT_ID,
	brokers: KAFKA_BROKERS,
	logLevel: logLevel.INFO,
	retry: {
		initialRetryTime: 300,
		retries: 10,
	},
});
