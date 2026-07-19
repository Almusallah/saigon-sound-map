# node >= 20.9 required by sharp (photo processing); 22 = current LTS.
FROM node:22-slim

# ffmpeg is used by /api/download/:id to transcode non-MP3 source audio
# (webm/opus from the browser recorder, m4a/mp4 from iOS uploads) into
# MP3 on the fly so downloaded files are universally playable. Image
# size grows ~150 MB but the transcoding step is otherwise free on the
# Render free tier (only invoked when a user clicks Download).
RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package*.json ./
RUN npm ci --production
COPY . .
EXPOSE 3000
CMD ["node", "server/index.js"]
