import { Router, Request, Response } from "express";
import { sendMessage } from "../producer";
import { TOPICS } from "../config";

const router = Router();

router.post("/receipts", async (req: Request, res: Response) => {
	try {
		const receipt = req.body as Record<string, unknown>;

		if (!receipt || Object.keys(receipt).length === 0) {
			return res
				.status(400)
				.json({ error: "Тело запроса не может быть пустым" });
		}
		const key = typeof receipt.id === "string" ? receipt.id : null;
		await sendMessage(TOPICS.RAW, receipt, key);

		return res.status(202).json({
			status: "accepted",
			message: "Чек принят и поставлен в очередь",
		});
	} catch (err) {
		return res
			.status(500)
			.json({ error: "Внутренняя ошибка сервера" });
	}
});

export default router;
