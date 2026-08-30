FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY index.js ./

# Mount point for auth state
VOLUME ["/app/baileys_auth_info"]

CMD ["npm", "start"]
