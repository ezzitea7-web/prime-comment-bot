FROM python:3.12-slim
WORKDIR /app
COPY bot.py campaigns.json /app/
ENV PYTHONUNBUFFERED=1 PORT=8080 BOT_DB=/data/bot.sqlite3 CAMPAIGN_FILE=/app/campaigns.json
EXPOSE 8080
CMD ["python","-c","import os; os.makedirs('/data', exist_ok=True); os.chown('/data', 10001, 10001); os.setgroups([]); os.setgid(10001); os.setuid(10001); os.execvp('python', ['python', 'bot.py'])"]
