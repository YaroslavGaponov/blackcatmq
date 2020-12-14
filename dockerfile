FROM node:13
WORKDIR /usr/blackcatmq
COPY . .
EXPOSE 61613
CMD [ "node", "blackcatmq.js" ]