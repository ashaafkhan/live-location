import http from 'node:http';
import path from 'node:path';

import 'dotenv/config';

import express from 'express';
import { Server } from 'socket.io';
import { cert, initializeApp } from 'firebase-admin/app';
import { getAuth } from 'firebase-admin/auth';

import { kafkaClient } from './kafka-client.js';

async function main() {
  const PORT = process.env.PORT ?? 8000;
  const firebaseConfig = {
    apiKey: process.env.FIREBASE_API_KEY,
    authDomain: process.env.FIREBASE_AUTH_DOMAIN,
    projectId: process.env.FIREBASE_PROJECT_ID,
    storageBucket: process.env.FIREBASE_STORAGE_BUCKET,
    messagingSenderId: process.env.FIREBASE_MESSAGING_SENDER_ID,
    appId: process.env.FIREBASE_APP_ID,
    measurementId: process.env.FIREBASE_MEASUREMENT_ID,
  };

  const serviceAccountRaw = process.env.FIREBASE_SERVICE_ACCOUNT;
  if (!serviceAccountRaw) {
    throw new Error(
      'Missing FIREBASE_SERVICE_ACCOUNT in .env (service account JSON).',
    );
  }

  const serviceAccount = JSON.parse(serviceAccountRaw);
  if (serviceAccount.private_key) {
    serviceAccount.private_key = serviceAccount.private_key.replace(
      /\\n/g,
      '\n',
    );
  }

  initializeApp({ credential: cert(serviceAccount) });

  const app = express();
  const server = http.createServer(app);
  const io = new Server(server);

  const kafkaProducer = kafkaClient.producer();
  await kafkaProducer.connect();

  const kafkaConsumer = kafkaClient.consumer({
    groupId: `socket-server-${PORT}`,
  });
  await kafkaConsumer.connect();

  await kafkaConsumer.subscribe({
    topics: ['location-updates'],
    fromBeginning: true,
  });

  kafkaConsumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      const data = JSON.parse(message.value.toString());
      console.log(`KafkaConsumer Data Received`, { data });
      io.emit('server:location:update', {
        id: data.id,
        latitude: data.latitude,
        longitude: data.longitude,
      });
      await heartbeat();
    },
  });


  io.use(async (socket, next) => {
    const token = socket.handshake.auth?.token;
    if (!token) {
      return next(new Error('Unauthorized'));
    }

    try {
      const decoded = await getAuth().verifyIdToken(token);
      socket.data.userId = decoded.uid;
      return next();
    } catch (error) {
      return next(new Error('Unauthorized'));
    }
  });

  io.on('connection', (socket) => {
    console.log(`[Socket:${socket.id}]: Connected Success...`);

    socket.on('client:location:update', async (locationData) => {
      const { latitude, longitude } = locationData;
      console.log(
        `[Socket:${socket.id}]:client:location:update:`,
        locationData,
      );

      await kafkaProducer.send({
        topic: 'location-updates',
        messages: [
          {
            key: socket.id,
            value: JSON.stringify({ id: socket.id, latitude, longitude }),
          },
        ],
      });
    });
  });

  app.use(express.static(path.resolve('./public')));

  app.get('/config', (req, res) => {
    return res.json({ firebaseConfig });
  });

  app.get('/health', (req, res) => {
    return res.json({ healthy: true });
  });

  server.listen(PORT, () =>
    console.log(`Server running on http://localhost:${PORT}`),
  );
}

main();
