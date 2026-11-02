import { createTopics } from './admin.js';
import { startValidator } from './validator.js';

async function main() {
    await createTopics();
    startValidator().catch(console.error);
}

main().catch(err => {
    console.error('❌ Ошибка:', err.message);
    process.exit(1);
});