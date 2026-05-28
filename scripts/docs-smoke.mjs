import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { chromium } from 'playwright';

const host = '127.0.0.1';
const port = 4329;
const origin = `http://${host}:${port}`;
const baseUrl = `${origin}/etl`;

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function waitForPreview() {
  for (let attempt = 0; attempt < 80; attempt += 1) {
    try {
      const response = await fetch(`${baseUrl}/`);
      if (response.ok) {
        return;
      }
    } catch {
      // Keep waiting until the preview server accepts connections.
    }

    await delay(250);
  }

  throw new Error('Docs preview server did not start in time.');
}

async function assertNoHorizontalOverflow(page) {
  const overflow = await page.evaluate(
    () => document.documentElement.scrollWidth - document.documentElement.clientWidth,
  );
  assert(overflow <= 1, `Expected no horizontal overflow, found ${overflow}px.`);
}

async function checkMobile(page) {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(`${baseUrl}/guides/first-pipeline/`, { waitUntil: 'load' });
  await assertNoHorizontalOverflow(page);

  const menuButton = page.locator('nav[aria-label="Main"] button[aria-controls="starlight__sidebar"]');
  await menuButton.click();
  await page.waitForFunction(
    () => getComputedStyle(document.querySelector('#starlight__sidebar')).visibility === 'visible',
  );
  await menuButton.click();
  await page.waitForFunction(
    () => getComputedStyle(document.querySelector('#starlight__sidebar')).visibility === 'hidden',
  );

  await page.locator('mobile-starlight-toc summary').click();
  await page.locator('mobile-starlight-toc a[href="#what-youll-build"]').click();
  await page.waitForFunction(() => !document.querySelector('#starlight__mobile-toc')?.hasAttribute('open'));
}

async function checkDesktop(page) {
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto(`${baseUrl}/explanation/architecture/`, { waitUntil: 'load' });
  await assertNoHorizontalOverflow(page);
  await page.waitForSelector('.mermaid svg', { timeout: 5000 });

  const activeSidebarItem = page.locator('#starlight__sidebar a[aria-current="page"]');
  await activeSidebarItem.waitFor({ state: 'visible' });
}

const preview = spawn(
  'npm',
  ['run', 'preview', '--', '--host', host, '--port', String(port)],
  {
    stdio: ['ignore', 'pipe', 'pipe'],
  },
);

preview.stdout.on('data', (chunk) => process.stdout.write(chunk));
preview.stderr.on('data', (chunk) => process.stderr.write(chunk));

let browser;

try {
  await waitForPreview();
  browser = await chromium.launch();
  const page = await browser.newPage();

  await checkMobile(page);
  await checkDesktop(page);

  console.log('Docs smoke checks passed.');
} finally {
  await browser?.close();
  preview.kill('SIGTERM');
}
