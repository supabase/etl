import { createReadStream } from 'node:fs';
import { stat } from 'node:fs/promises';
import { createServer } from 'node:http';
import { extname, resolve, sep } from 'node:path';

const args = process.argv.slice(2);
const option = (name, fallback) => {
  const index = args.indexOf(name);
  return index === -1 ? fallback : args[index + 1];
};

const host = option('--host', '127.0.0.1');
const port = Number(option('--port', '3000'));
const basePath = '/etl';
const exportDirectory = resolve('out');
const contentTypes = new Map([
  ['.css', 'text/css; charset=utf-8'],
  ['.html', 'text/html; charset=utf-8'],
  ['.ico', 'image/x-icon'],
  ['.js', 'text/javascript; charset=utf-8'],
  ['.json', 'application/json; charset=utf-8'],
  ['.map', 'application/json; charset=utf-8'],
  ['.md', 'text/markdown; charset=utf-8'],
  ['.png', 'image/png'],
  ['.svg', 'image/svg+xml'],
  ['.txt', 'text/plain; charset=utf-8'],
  ['.webmanifest', 'application/manifest+json; charset=utf-8'],
  ['.woff2', 'font/woff2'],
  ['.xml', 'application/xml; charset=utf-8'],
]);

function contentType(file, pathname) {
  if (pathname === `${basePath}/api/search`) return 'application/json; charset=utf-8';
  return contentTypes.get(extname(file)) ?? 'application/octet-stream';
}

async function resolveFile(pathname) {
  const relativePath = pathname.slice(basePath.length) || '/';
  const requested = resolve(exportDirectory, `.${relativePath}`);
  if (requested !== exportDirectory && !requested.startsWith(`${exportDirectory}${sep}`)) return;

  const candidates = relativePath.endsWith('/')
    ? [resolve(requested, 'index.html')]
    : [requested, resolve(requested, 'index.html')];

  for (const candidate of candidates) {
    try {
      if ((await stat(candidate)).isFile()) return candidate;
    } catch {
      // Try the next static-export path shape.
    }
  }
}

const server = createServer(async (request, response) => {
  const url = new URL(request.url ?? '/', `http://${host}:${port}`);

  if (url.pathname === basePath) {
    response.writeHead(308, { Location: `${basePath}/` });
    response.end();
    return;
  }

  if (!url.pathname.startsWith(`${basePath}/`)) {
    response.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    response.end(`Open ${basePath}/ to preview the exported documentation.`);
    return;
  }

  const file = await resolveFile(decodeURIComponent(url.pathname));
  if (!file) {
    response.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    response.end('Not found');
    return;
  }

  const immutable = url.pathname.includes('/_next/static/');
  response.writeHead(200, {
    'Content-Type': contentType(file, url.pathname),
    'Cache-Control': immutable ? 'public, max-age=31536000, immutable' : 'no-cache',
  });
  createReadStream(file).pipe(response);
});

server.listen(port, host, () => {
  console.log(`ETL docs preview: http://${host}:${port}${basePath}/`);
});

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => server.close(() => process.exit(0)));
}
