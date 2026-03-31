import express from "express";
import { createServer } from "http";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function startServer() {
  const app = express();
  const server = createServer(app);

  // Proxy /api/* requests to FastAPI backend (port 8000)
  const PYTHON_API_URL = process.env.PYTHON_API_URL || "http://localhost:8000";

  app.use("/api", async (req, res) => {
    try {
      const targetUrl = `${PYTHON_API_URL}${req.originalUrl}`;

      // Collect raw body for non-GET/HEAD requests
      const chunks: Buffer[] = [];
      if (req.method !== "GET" && req.method !== "HEAD") {
        for await (const chunk of req) {
          chunks.push(chunk);
        }
      }
      const rawBody = Buffer.concat(chunks);

      // Detect multipart requests (file uploads) and forward content-type as-is
      const incomingContentType = req.headers["content-type"] || "";
      const isMultipart = incomingContentType.startsWith("multipart/");

      const fetchHeaders: Record<string, string> = {};
      if (isMultipart) {
        // Forward the original content-type with boundary intact
        fetchHeaders["Content-Type"] = incomingContentType;
      } else if (rawBody.length > 0) {
        fetchHeaders["Content-Type"] = "application/json";
      }
      if (req.headers.authorization) {
        fetchHeaders["Authorization"] = req.headers.authorization;
      }

      const fetchOptions: RequestInit = {
        method: req.method,
        headers: fetchHeaders,
      };

      if (rawBody.length > 0) {
        fetchOptions.body = isMultipart ? rawBody : rawBody.toString();
      }

      const apiResponse = await fetch(targetUrl, fetchOptions);
      res.status(apiResponse.status);

      // Forward response headers
      apiResponse.headers.forEach((value, key) => {
        if (key.toLowerCase() !== "transfer-encoding") {
          res.setHeader(key, value);
        }
      });

      // Detect binary responses (PDF, etc.) and forward as buffer
      const responseContentType = apiResponse.headers.get("content-type") || "";
      const isBinaryResponse =
        responseContentType.includes("application/pdf") ||
        responseContentType.includes("application/octet-stream") ||
        responseContentType.includes("application/zip") ||
        responseContentType.includes("application/vnd.");

      if (isBinaryResponse) {
        const buffer = Buffer.from(await apiResponse.arrayBuffer());
        res.send(buffer);
      } else {
        const body = await apiResponse.text();
        res.send(body);
      }
    } catch (error) {
      console.error("API proxy error:", error);
      res.status(502).json({
        status: "error",
        detail: "Não foi possível conectar ao backend Python. Verifique se o servidor FastAPI está rodando.",
      });
    }
  });

  // Serve static files from dist/public in production
  const staticPath =
    process.env.NODE_ENV === "production"
      ? path.resolve(__dirname, "public")
      : path.resolve(__dirname, "..", "dist", "public");

  app.use(express.static(staticPath));

  // Handle client-side routing - serve index.html for all non-API routes
  app.get("*", (_req, res) => {
    res.sendFile(path.join(staticPath, "index.html"));
  });

  const port = process.env.PORT || 3000;

  server.listen(port, () => {
    console.log(`Server running on http://localhost:${port}/`);
    console.log(`API proxy → ${PYTHON_API_URL}`);
  });
}

startServer().catch(console.error);
