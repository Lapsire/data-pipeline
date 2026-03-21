FROM node:18-alpine

# Code dir
WORKDIR /usr/src/app

COPY package*.json ./

# Install dependencies
RUN npm install

COPY . .

EXPOSE 3000

CMD ["node", "index.js"]