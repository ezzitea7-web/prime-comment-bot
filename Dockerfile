FROM python:3.12-slim
WORKDIR /app
COPY bot.py campaigns.json /app/
ENV PYTHONUNBUFFERED=1 PORT=8080 BOT_DB=/data/bot.sqlite3 CAMPAIGN_FILE=/app/campaigns.json
USER 10001:10001
EXPOSE 8080
CMD ["python", "bot.py"]
