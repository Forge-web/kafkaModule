import { createTopics } from "./admin.js";
import express from "express";
import messageRouter from "./routes/receipts.js";
import { connectProducer, disconnectProducer } from "./producer.js";
import { startConsumer, stopConsumer } from "./consumer.js";

const app = express();
app.use(express.json());
app.use("/api", messageRouter);

app.get("/health", (_req, res) => res.json({ status: "ok" }));

const PORT = Number(process.env.PORT ?? 3100);

async function bootstrap(): Promise<void> {
	try {
		await createTopics();
		await connectProducer();
		await startConsumer();

		const server = app.listen(PORT, () => {
			console.log(`HTTP сервер запущен на порту ${PORT}`);
		});

		const shutdown = async (signal: string): Promise<void> => {
			console.log(`\nОстановка сервиса...`);
			server.close(async () => {
				await stopConsumer();
				await disconnectProducer();
				process.exit(0);
			});
			setTimeout(() => process.exit(1), 10_000).unref();
		};

		process.on("SIGINT", () => void shutdown("SIGINT"));
		process.on("SIGTERM", () => void shutdown("SIGTERM"));
	} catch (err) {
		console.error(`Ошибка при запуске:\t${err}`);
		process.exit(1);
	}
}

void bootstrap();
