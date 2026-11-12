export type ValidateResult =
	| { valid: true; data: any }
	| { valid: false; data: any; errors: string[] };

export interface DeadLetterMessage {
	originalMessage: unknown;
	errors: string[];
}
