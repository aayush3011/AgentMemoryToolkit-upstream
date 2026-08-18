FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    AGENT_MEMORY_MCP_TRANSPORT=streamable-http \
    AGENT_MEMORY_MCP_HOST=0.0.0.0 \
    AGENT_MEMORY_MCP_PORT=8080 \
    AGENT_MEMORY_MCP_HTTP_PATH=/mcp

WORKDIR /app

RUN addgroup --system app && adduser --system --ingroup app app

COPY pyproject.toml README.md ./
COPY src ./src

RUN python -m pip install --upgrade pip && \
    python -m pip install '.[auth]'

USER app
EXPOSE 8080

CMD ["agent-memory-mcp"]
