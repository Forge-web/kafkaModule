export const KAFKA_BROKERS = ["localhost:9092"];
export const CLIENT_ID = "receipts_system";

export const TOPICS = {
	RAW: "raw_receipts",
	VALID: "valid_receipts",
	INVALID: "invalid_receipts",
} as const;
