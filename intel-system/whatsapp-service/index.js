const { makeWASocket, useMultiFileAuthState, DisconnectReason } = require('@whiskeysockets/baileys');
const { Boom } = require('@hapi/boom');
const pino = require('pino');
const axios = require('axios');
const qrcode = require('qrcode-terminal');
const fs = require('fs');
const path = require('path');

// Support both Docker (/app/baileys_auth_info) and local node (./baileys_auth_info)
const AUTH_DIR = process.env.AUTH_DIR || path.join(__dirname, 'baileys_auth_info');
if (!fs.existsSync(AUTH_DIR)) {
    fs.mkdirSync(AUTH_DIR, { recursive: true });
}

const INGEST_URL = process.env.INGEST_URL || 'http://127.0.0.1:8000/ingest/whatsapp';
const INGEST_TOKEN = process.env.INGEST_TOKEN || 'intel_dev_secret'; 

// Optional: comma-separated list of tracked Group JIDs (e.g. "123456@g.us,987654@g.us")
// If empty, service operates in DISCOVERY MODE and forwards all group messages.
const TRACKED_GROUPS = process.env.TRACKED_GROUPS 
    ? process.env.TRACKED_GROUPS.split(',').map(s => s.trim()).filter(Boolean)
    : [];

async function connectToWhatsApp() {
    console.log(`[INIT] Loading auth state from: ${AUTH_DIR}`);
    const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);

    const sock = makeWASocket({
        auth: state,
        printQRInTerminal: false, // We handle printing explicitly below
        logger: pino({ level: 'silent' }),
        browser: ['Daily Intel', 'Chrome', '1.0.0'],
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect, qr } = update;
        
        if (qr) {
            console.log('\n======================================================');
            console.log(' [QR CODE] SCAN THIS WITH YOUR DUMMY WHATSAPP ACCOUNT');
            console.log(' Open WhatsApp > Settings > Linked Devices > Link a Device');
            console.log('======================================================\n');
            qrcode.generate(qr, { small: true });
            console.log('\n======================================================\n');
        }

        if (connection === 'close') {
            const statusCode = (lastDisconnect.error instanceof Boom)?.output?.statusCode;
            const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
            console.log(`[SYSTEM] Connection closed (code: ${statusCode}), reconnecting: ${shouldReconnect}`);
            
            if (shouldReconnect) {
                setTimeout(connectToWhatsApp, 3000);
            } else {
                console.log('[SYSTEM] Logged out. Delete the auth folder to generate a new QR code.');
            }
        } else if (connection === 'open') {
            console.log('======================================================');
            console.log(' [SUCCESS] WhatsApp connected and listening for messages!');
            if (TRACKED_GROUPS.length > 0) {
                console.log(` [FILTER] Monitoring ${TRACKED_GROUPS.length} specific groups: ${TRACKED_GROUPS.join(', ')}`);
            } else {
                console.log(' [DISCOVERY MODE] Monitoring ALL groups. Send messages to discover group JIDs.');
            }
            console.log('======================================================');
        }
    });

    sock.ev.on('messages.upsert', async (m) => {
        if (m.type !== 'notify') return;

        for (const msg of m.messages) {
            if (msg.key.fromMe) continue;
            
            const jid = msg.key.remoteJid;
            if (!jid || !jid.endsWith('@g.us')) continue; // Group messages only

            const text = msg.message?.conversation || 
                         msg.message?.extendedTextMessage?.text || 
                         msg.message?.imageMessage?.caption ||
                         msg.message?.videoMessage?.caption ||
                         "";

            const sender = msg.pushName || msg.key.participant || "Unknown";
            console.log(`\n[MESSAGE RECEIVED] Group JID: ${jid} | Sender: ${sender}`);
            console.log(`Preview: "${text.slice(0, 80)}${text.length > 80 ? '...' : ''}"`);

            // If specific groups are configured, filter out unlisted ones
            if (TRACKED_GROUPS.length > 0 && !TRACKED_GROUPS.includes(jid)) {
                console.log(`[SKIPPED] Group ${jid} is not in TRACKED_GROUPS list.`);
                continue;
            }

            if (!text.trim()) {
                console.log(`[SKIPPED] Message in ${jid} contains no text or caption.`);
                continue;
            }

            const payload = {
                source_identifier: jid,
                items: [
                    {
                        external_id: msg.key.id,
                        occurred_at: new Date((msg.messageTimestamp || Math.floor(Date.now() / 1000)) * 1000).toISOString(),
                        url: null,
                        text: text,
                        payload: msg
                    }
                ]
            };

            try {
                const response = await axios.post(INGEST_URL, payload, {
                    headers: {
                        'X-Source-Type': 'whatsapp_group',
                        'X-Ingest-Token': INGEST_TOKEN,
                        'Content-Type': 'application/json'
                    },
                    timeout: 10000
                });
                console.log(`[FORWARDED] Successfully saved message to Daily Intel backend (${response.data.accepted} accepted, ${response.data.duplicates} dupes).`);
            } catch (error) {
                console.error(`[ERROR] Ingest forwarding failed for ${jid}: ${error.response?.data?.detail || error.message}`);
            }
        }
    });
}

connectToWhatsApp();

