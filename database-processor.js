import fs from 'node:fs/promises';
import path from 'node:path';

import { kafkaClient } from './kafka-client.js';

const logFilePath = path.resolve('./location-log.json');
const locationLog = [];

async function init() {
  const kafkaConsumer = kafkaClient.consumer({
    groupId: `database-processor`,
  });
  await kafkaConsumer.connect();

  await kafkaConsumer.subscribe({
    topics: ['location-updates'],
    fromBeginning: true,
  });

  kafkaConsumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      const data = JSON.parse(message.value.toString());
      const entry = {
        ...data,
        receivedAt: new Date().toISOString(),
      };

      locationLog.push(entry);
      console.log(`INSERT INTO DB LOCATION`, entry);

      // Writing directly on every socket event would overwhelm a DB at scale.
      // Kafka buffers events; this consumer can batch or throttle writes.
      await fs.writeFile(logFilePath, JSON.stringify(locationLog, null, 2));
      await heartbeat();
    },
  });
}

init();
