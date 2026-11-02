// kafkaModule/src/server.ts
import express from 'express';
import { sendMessage } from './producer.js';
import { TOPICS } from './config.js';
import { ALL } from 'node:dns';

const app = express();
const PORT = 3001;

// 🟢 Middleware для парсинга JSON (обязательно!)
app.use(express.json());

// 🟢 Ручка для приема чека
app.post('/receiptsTest', async (req, res) => {
  const receipt = req.body;

  if (!receipt) {
    return res.status(400).json({ error: 'Пустое тело запроса' });
  }

  try {
    // Отправляем чек в Kafka (raw_receipts)
    await sendMessage(TOPICS.RAW, receipt);
    
    console.log('✅ Чек принят и отправлен в Kafka');
    return res.json({ 
      success: true, 
      message: 'Чек принят и отправлен в Kafka' 
    });
  } catch (error: any) {
    console.error('❌ Ошибка отправки в Kafka:', error);
    return res.status(500).json({ error: error.message });
  }
});

// 🟢 GET-ручка для вывода всех чеков (заглушка)
app.get('/receiptsTest', (req, res) => {
  // Пока возвращаем фейковые данные
  const mockReceipts = [
    { id: '1', check_sum: 200, status: 'valid' },
    { id: '2', check_sum: 500, status: 'invalid' },
  ];
  
  res.json({ 
    success: true, 
    count: mockReceipts.length,
    data: mockReceipts 
  });
});

// 🟢 Запуск сервера
app.listen(PORT, () => {
  console.log(`🚀 kafkaModule REST API запущен на http://localhost:${PORT}`);
});